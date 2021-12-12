package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.annotation.JsonInclude;
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

import static java.util.stream.Collectors.toList;

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
    private Schema keySchema;
    private final Schema valueSchema;

    private List<String> keys;

    public RecordConverter(TableDescription tableDesc, String topicNamePrefix) {
        this.tableDesc = tableDesc;
        this.topic_name = topicNamePrefix + tableDesc.getTableName();

        valueSchema = SchemaBuilder.struct()
                                   .name(SchemaNameAdjuster.DEFAULT.adjust( "com.trustpilot.connector.dynamodb.envelope"))
                                   .field(Envelope.FieldName.VERSION, Schema.STRING_SCHEMA)
                                   .field(Envelope.FieldName.DOCUMENT, DynamoDbJson.schema())
                                   .field(Envelope.FieldName.SOURCE, SourceInfo.structSchema())
                                   .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                                   .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
                                   .build();
    }

    public SourceRecord toSourceRecord(
            SourceInfo sourceInfo,
            Envelope.Operation op,
            Map<String, AttributeValue> attributes,
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

        // DynamoDB keys can be changed only by recreating the table
        if (keySchema == null) {
            keys = tableDesc.getKeySchema().stream().map(this::sanitiseAttributeName).collect(toList());
            keySchema = getKeySchema(keys);
        }

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
                .put(Envelope.FieldName.TIMESTAMP, arrivalTimestamp.toEpochMilli());

        return new SourceRecord(
                Collections.singletonMap("table_name", sourceInfo.tableName),
                offsets,
                topic_name,
                keySchema, keyData,
                valueSchema, valueData
        );
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
