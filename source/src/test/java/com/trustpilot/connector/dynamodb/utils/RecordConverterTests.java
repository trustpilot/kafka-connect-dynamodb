


package com.trustpilot.connector.dynamodb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.trustpilot.connector.dynamodb.Envelope;
import com.trustpilot.connector.dynamodb.InitSyncStatus;
import com.trustpilot.connector.dynamodb.SourceInfo;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SuppressWarnings("SameParameterValue")
public class RecordConverterTests {

    private static final String table = "TestTable1";

    private TableDescription getTableDescription(List<KeySchemaElement> keySchema) {
        TableDescription description = new TableDescription();
        description.setTableName(table);

        if (keySchema == null) {
            keySchema = new LinkedList<>();
        }
        description.setKeySchema(keySchema);

        return description;
    }

    private Map<String, AttributeValue> getAttributes() {
        Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("testKV1", new AttributeValue().withS("testKV1Value"));
        attributes.put("testKV2", new AttributeValue().withS("2"));
        attributes.put("testV1", new AttributeValue().withN("1"));
        attributes.put("testV2", new AttributeValue().withS("testStringValue"));

        return attributes;
    }

    private Map<String, AttributeValue> getAttributesWithInvalidAvroCharacters() {
        Map<String, AttributeValue> attributes = new HashMap<>();
        attributes.put("test-1234", new AttributeValue().withS("testKV1Value"));
        attributes.put("1-starts-with-number", new AttributeValue().withS("2"));
        attributes.put("_starts_with_underscore", new AttributeValue().withN("1"));
        attributes.put("test!@£$%^", new AttributeValue().withS("testStringValue"));

        return attributes;
    }



    private SourceInfo getSourceInfo(String table) {
        SourceInfo sourceInfo = new SourceInfo(table, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));
        sourceInfo.initSyncStatus = InitSyncStatus.RUNNING;
        sourceInfo.lastInitSyncStart = Instant.parse("2001-01-01T00:00:00.00Z");

        return sourceInfo;
    }

    @Test
    public void correctTopicNameIsConstructed() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("TestTopicPrefix-TestTable1", record.topic());
    }

    @Test
    public void sourceInfoIsPutToOffset() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals(table, record.sourceOffset().get("table_name"));
    }

    @Test
    public void shardIdAndSequenceNumberIsPutToOffset() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("testShardID1", record.sourceOffset().get("src_shard_id"));
        assertEquals("testSequenceNumberID1", record.sourceOffset().get("src_shard_sequence_no"));
    }

    @Test
    public void singleItemKeyIsAddedToRecord() throws Exception {
        // Arrange
        List<KeySchemaElement> keySchema = new LinkedList<>();
        keySchema.add(new KeySchemaElement().withKeyType("S").withAttributeName("testKV1"));

        RecordConverter converter = new RecordConverter(getTableDescription(keySchema), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("testKV1", record.keySchema().fields().get(0).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(0).schema());
        assertEquals("testKV1Value", ((Struct) record.key()).getString("testKV1"));
    }

    @Test
    public void multiItemKeyIsAddedToRecord() throws Exception {
        // Arrange
        List<KeySchemaElement> keySchema = new LinkedList<>();
        keySchema.add(new KeySchemaElement().withKeyType("S").withAttributeName("testKV1"));
        keySchema.add(new KeySchemaElement().withKeyType("N").withAttributeName("testKV2"));

        RecordConverter converter = new RecordConverter(getTableDescription(keySchema), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("testKV1", record.keySchema().fields().get(0).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(0).schema());
        assertEquals("testKV1Value", ((Struct) record.key()).getString("testKV1"));

        assertEquals("testKV2", record.keySchema().fields().get(1).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(1).schema());
        assertEquals("2", ((Struct) record.key()).getString("testKV2"));
    }

    @Test
    public void recordAttributesAreAddedToValueData() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("{\"testKV1\":{\"s\":\"testKV1Value\"},\"testKV2\":{\"s\":\"2\"},\"testV2\":{\"s\":\"testStringValue\"},\"testV1\":{\"n\":\"1\"}}",
                     ((Struct) record.value()).getString("document"));
    }

    @Test
    public void singleItemKeyIsAddedToRecordWhenKeyContainsInvalidCharacters() throws Exception {
        // Arrange
        List<KeySchemaElement> keySchema = new LinkedList<>();
        keySchema.add(new KeySchemaElement().withKeyType("S").withAttributeName("test-1234"));

        RecordConverter converter = new RecordConverter(getTableDescription(keySchema), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributesWithInvalidAvroCharacters(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("test1234", record.keySchema().fields().get(0).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(0).schema());
        assertEquals("testKV1Value", ((Struct) record.key()).getString("test1234"));
    }

    @Test
    public void multiItemKeyIsAddedToRecordWhenKeyContainsInvalidCharacters() throws Exception {
        // Arrange
        List<KeySchemaElement> keySchema = new LinkedList<>();
        keySchema.add(new KeySchemaElement().withKeyType("S").withAttributeName("test-1234"));
        keySchema.add(new KeySchemaElement().withKeyType("N").withAttributeName("1-starts-with-number"));

        RecordConverter converter = new RecordConverter(getTableDescription(keySchema), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributesWithInvalidAvroCharacters(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("test1234", record.keySchema().fields().get(0).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(0).schema());
        assertEquals("testKV1Value", ((Struct) record.key()).getString("test1234"));

        assertEquals("startswithnumber", record.keySchema().fields().get(1).name());
        assertEquals(SchemaBuilder.string().build(), record.keySchema().fields().get(1).schema());
        assertEquals("2", ((Struct) record.key()).getString("startswithnumber"));
    }

    @Test
    public void recordAttributesAreAddedToValueDataWhenAttributesContainsInvalidCharacters() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributesWithInvalidAvroCharacters(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        String expected = "{\"test1234\":{\"s\":\"testKV1Value\"},\"_starts_with_underscore\":{\"n\":\"1\"},\"startswithnumber\":{\"s\":\"2\"},\"test\":{\"s\":\"testStringValue\"}}";

        // Assert
        assertEquals(expected,
                ((Struct) record.value()).getString("document"));
    }

    @Test
    public void sourceInfoIsAddedToValueData() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        Struct sourceInfoStruct = ((Struct) record.value()).getStruct("source");
        assertEquals(table, sourceInfoStruct.getString("table_name"));
        assertEquals("RUNNING", sourceInfoStruct.getString("init_sync_state"));
        assertEquals(978307200000L, sourceInfoStruct.getInt64("init_sync_start"));
    }

    @Test
    public void operationIsAddedToValueData() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals("r", ((Struct) record.value()).getString("op"));
    }

    @Test
    public void arrivalTimestampIsAddedToValueData() throws Exception {
        // Arrange
        RecordConverter converter = new RecordConverter(getTableDescription(null), "TestTopicPrefix-");

        // Act
        SourceRecord record = converter.toSourceRecord(
                getSourceInfo(table),
                Envelope.Operation.forCode("r"),
                getAttributes(),
                Instant.parse("2001-01-02T00:00:00.00Z"),
                "testShardID1",
                "testSequenceNumberID1"
        );

        // Assert
        assertEquals(978393600000L, ((Struct) record.value()).getInt64("ts_ms"));
    }

}
