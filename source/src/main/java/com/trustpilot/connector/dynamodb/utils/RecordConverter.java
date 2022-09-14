package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
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
import java.util.HashMap;
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
    private Schema valueSchema;

    private List<String> keys;

    public RecordConverter(TableDescription tableDesc, String topicNamePrefix) {
        this.tableDesc = tableDesc;
        this.topic_name = topicNamePrefix;          

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

        // getUnmarshallItems from Dynamo Document
        //Map<String, Object> unMarshalledItems = ItemUtils.toSimpleMapValue(attributes);

         //JSON conversion
        //String outputJsonString = ItemUtils.toItem(attributes).toJSON();   
        Struct dynamoAttributes = getAttributeValueStruct(sanitisedAttributes);          

        valueSchema = SchemaBuilder.struct()
        .name(SchemaNameAdjuster.DEFAULT.adjust( "com.trustpilot.connector.dynamodb.envelope"))
        .field(Envelope.FieldName.VERSION, Schema.STRING_SCHEMA)
        .field(Envelope.FieldName.DOCUMENT, getAttributeValueSchema(sanitisedAttributes))
        .field(Envelope.FieldName.SOURCE, SourceInfo.structSchema())
        .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
        .field(Envelope.FieldName.TIMESTAMP, Schema.INT64_SCHEMA)
        .build();

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
                .put(Envelope.FieldName.DOCUMENT, dynamoAttributes) // objectMapper.writeValueAsString(outputJsonString))
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

    public static Struct getAttributeValueStruct(Map<String, AttributeValue> attributes) {        
        final Struct attributeValueStruct = new Struct(getAttributeValueSchema(attributes));
        
        // Mapping dynamo db attributes to schema registry types (dynamo db attributes are documented at below link)
        //https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/model/AttributeValue.java

        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            if (attributeValue.getS() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getS());
            } else if (attributeValue.getN() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getN());
            } else if (attributeValue.getB() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getB());
            } else if (attributeValue.getSS() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getSS());
            } else if (attributeValue.getNS() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getNS());
            } else if (attributeValue.getBS() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getBS());
            } else if (attributeValue.getNULL() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getNULL());
            } else if (attributeValue.getBOOL() != null) {
                attributeValueStruct.put(attributeName, attributeValue.getBOOL());
            }           
        }
        return attributeValueStruct;
    }

    public static Schema getAttributeValueSchema(Map<String, AttributeValue> attributes) {        
        SchemaBuilder RECORD_ATTRIBUTES_SCHEMA = SchemaBuilder.struct().name("DynamoDB.AttributeValue");

        // Mapping dynamo db attributes to schema registry types (dynamo db attributes are documented at below link)
        //https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-dynamodb/src/main/java/com/amazonaws/services/dynamodbv2/model/AttributeValue.java

        for (Map.Entry<String, AttributeValue> attribute : attributes.entrySet()) {
            final String attributeName = attribute.getKey();
            final AttributeValue attributeValue = attribute.getValue();
            if (attributeValue.getS() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, Schema.STRING_SCHEMA);
            } else if (attributeValue.getN() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, Schema.STRING_SCHEMA);
            } else if (attributeValue.getB() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, Schema.BYTES_SCHEMA);
            } else if (attributeValue.getSS() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, SchemaBuilder.array(Schema.STRING_SCHEMA));
            } else if (attributeValue.getNS() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, SchemaBuilder.array(Schema.STRING_SCHEMA));
            } else if (attributeValue.getBS() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, SchemaBuilder.array(Schema.BYTES_SCHEMA));
            } else if (attributeValue.getNULL() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, Schema.BOOLEAN_SCHEMA);
            } else if (attributeValue.getBOOL() != null) {
                RECORD_ATTRIBUTES_SCHEMA.field(attributeName, Schema.BOOLEAN_SCHEMA);
            }           
        }
        return RECORD_ATTRIBUTES_SCHEMA.build();
    }
    
}
