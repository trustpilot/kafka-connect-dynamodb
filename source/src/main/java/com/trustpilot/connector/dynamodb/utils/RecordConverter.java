package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.trustpilot.connector.dynamodb.Envelope;
import com.trustpilot.connector.dynamodb.SourceInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

import java.io.IOException;

/**
 * Takes in KCL event attributes and converts into Kafka Connect Source record.
 * With dynamic schema for key(based on actual DynamoDB table keys) and fixed schema for value.
 */
public class RecordConverter {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static final String SHARD_ID = "src_shard_id";
    public static final String SHARD_SEQUENCE_NO = "src_shard_sequence_no";

    private final TableDescription tableDesc;
    private final String topic_name;
    private final Schema keySchema;
    private final Schema valueSchema;

    private List<String> keys;

    public RecordConverter(TableDescription tableDesc, String topicNamePrefix) {
        this(tableDesc, topicNamePrefix, null);
    }

    public RecordConverter(TableDescription tableDesc, String topicNamePrefix, String topicNamespaceMap) {
        this.tableDesc = tableDesc;
        this.topic_name = topicNamePrefix + this.getTopicNameSuffix(topicNamespaceMap, tableDesc.getTableName());

        this.keys = tableDesc.getKeySchema().stream().map(this::sanitiseAttributeName).collect(toList());
        this.keySchema = getKeySchema(keys);
        this.valueSchema = SchemaBuilder.struct()
                                   .name(SchemaNameAdjuster.DEFAULT.adjust( "com.trustpilot.connector.dynamodb.envelope"))
                                   .field(Envelope.FieldName.VERSION, Schema.STRING_SCHEMA)
                                   .field(Envelope.FieldName.EVENT_ID, Schema.OPTIONAL_STRING_SCHEMA)
                                   .field(Envelope.FieldName.DOCUMENT, DynamoDbJson.schema())
                                   .field(Envelope.FieldName.OLD_DOCUMENT, DynamoDbJson.optionalSchema())
                                   .field(Envelope.FieldName.SOURCE, SourceInfo.structSchema())
                                   .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                                   .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                                   .field(Envelope.FieldName.KEY, this.keySchema)
                                   .build();
    }

    public SourceRecord toSourceRecord(
            SourceInfo sourceInfo,
            Envelope.Operation op,
            Map<String, AttributeValue> attributes,
            Instant arrivalTimestamp,
            String shardId,
            String sequenceNumber) throws Exception{

            return toSourceRecord(sourceInfo, op, null, attributes, null, arrivalTimestamp, shardId, sequenceNumber);
    }

    public SourceRecord toSourceRecord(
            SourceInfo sourceInfo,
            Envelope.Operation op,
            String eventId,
            Map<String, AttributeValue> attributes,
            Map<String, AttributeValue> oldAttributes,
            Instant arrivalTimestamp,
            String shardId,
            String sequenceNumber) throws Exception {

        // Sanitise the incoming attributes to remove any invalid Avro characters
        final Map<String, AttributeValue> sanitisedAttributes = attributes.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> this.sanitiseAttributeName(e.getKey()),
                        Map.Entry::getValue,
                        (u, v) -> u,
                        LinkedHashMap::new
                ));


        // Leveraging offsets to store shard and sequence number with each item pushed to Kafka.
        // This info will only be used to update `shardRegister` and won't be used to reset state after restart
        Map<String, Object> offsets = SourceInfo.toOffset(sourceInfo);
        offsets.put(SHARD_ID, shardId);
        offsets.put(SHARD_SEQUENCE_NO, sequenceNumber);

        Struct keyData = new Struct(getKeySchema(keys));
        for (String key : keys) {
            AttributeValue attributeValue = sanitisedAttributes.get(key);
            if (attributeValue.getS() != null) {
                keyData.put(key, attributeValue.getS());
                continue;
            } else if (attributeValue.getN() != null) {
                keyData.put(key, attributeValue.getN());
                continue;
            }
            throw new Exception("Unsupported key AttributeValue");
        }

        Struct valueData = new Struct(valueSchema)
                .put(Envelope.FieldName.VERSION, sourceInfo.version)
                .put(Envelope.FieldName.DOCUMENT, objectMapper.writeValueAsString(sanitisedAttributes))
                .put(Envelope.FieldName.SOURCE, SourceInfo.toStruct(sourceInfo))
                .put(Envelope.FieldName.OPERATION, op.code())
                .put(Envelope.FieldName.TIMESTAMP, arrivalTimestamp.toEpochMilli())
                .put(Envelope.FieldName.KEY, keyData);


        if (eventId != null) {
            valueData = valueData.put(Envelope.FieldName.EVENT_ID, eventId);
        }
        if (oldAttributes != null) {
            Map<String, AttributeValue> sanitisedOldAttributes = oldAttributes.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> this.sanitiseAttributeName(e.getKey()),
                        Map.Entry::getValue,
                        (u, v) -> u,
                        LinkedHashMap::new
                ));
            valueData = valueData.put(Envelope.FieldName.OLD_DOCUMENT, objectMapper.writeValueAsString(sanitisedOldAttributes));
        }

        return new SourceRecord(
                Collections.singletonMap("table_name", sourceInfo.tableName),
                offsets,
                topic_name,
                keySchema, keyData,
                valueSchema, valueData
        );
    }

    private String getTopicNameSuffix(String topicNamespaceMap, String tableName) {
        if (Strings.isNullOrEmpty(topicNamespaceMap)) {
            return tableName;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            LinkedHashMap<String, Object> map = objectMapper.readValue(topicNamespaceMap, new TypeReference<LinkedHashMap<String, Object>>() {});

            if (map.containsKey(tableName)) {
                return (String) map.get(tableName);
            }
            
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid topicNamespaceMap: " + topicNamespaceMap);
        }

        return tableName;
    }

    private Schema getKeySchema(List<String> keys) {
        SchemaBuilder keySchemaBuilder = SchemaBuilder.struct()
                                                      .name(SchemaNameAdjuster.DEFAULT.adjust(topic_name + ".Key"));
        for (String key : keys) {
            keySchemaBuilder.field(key, Schema.STRING_SCHEMA);
        }

        return keySchemaBuilder.build();
    }

    private String sanitiseAttributeName(KeySchemaElement element) {
        return this.sanitiseAttributeName(element.getAttributeName());
    }

    private String sanitiseAttributeName(final String attributeName) {
        final String sanitisedAttributeName = attributeName.replaceAll("^[^a-zA-Z_]|(?<!^)[^a-zA-Z0-9_]", "");

        if (Strings.isNullOrEmpty(sanitisedAttributeName)) {
            throw new IllegalStateException(String.format("The field name %s couldn't be sanitised correctly", attributeName));
        }

        return sanitisedAttributeName;
    }
}
