package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.trustpilot.connector.dynamodb.aws.TableScanner;
import com.trustpilot.connector.dynamodb.kcl.KclRecordsWrapper;
import com.trustpilot.connector.dynamodb.kcl.KclWorker;
import com.trustpilot.connector.dynamodb.kcl.ShardInfo;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("ConstantConditions")
public class DynamoDBSourceTaskTests {
    private final static String tableName = "testTable1";

    private HashMap<String, String> configs;

    @BeforeEach
    private void beforeEach() {
        configs = new HashMap<>();
        configs.put("table", tableName);
        configs.put("task.id", "testTask1");
        configs.put("aws.region", "testRegion");
        configs.put("init.sync.delay.period", "0");
    }

    private class SourceTaskBuilder {
        public HashMap<String, Object> offset;
        public TableScanner tableScanner;
        public KclWorker kclWorker;

        public StubContext context;
        public TableDescription tableDescription;

        public Clock clock;

        public List<Map<String, AttributeValue>> initSyncRecords;
        public Map<String, AttributeValue> exclusiveStartKey = null;
        public List<KclRecordsWrapper> syncRecords;


        public SourceTaskBuilder withOffset(HashMap<String, Object> offset) {
            this.offset = offset;
            return this;
        }

        public SourceTaskBuilder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public SourceTaskBuilder withTableDescription(TableDescription tableDescription) {
            this.tableDescription = tableDescription;
            return this;
        }

        public SourceTaskBuilder withInitSyncRecords(List<Map<String, AttributeValue>> initSyncRecords,
                                                     Map<String, AttributeValue> exclusiveStartKey) {
            this.initSyncRecords = initSyncRecords;
            this.exclusiveStartKey = exclusiveStartKey;
            return this;
        }

        public DynamoDBSourceTask buildTask() throws InterruptedException {
            // Setup metadata
            StubOffsetStorageReader offsetReader = new StubOffsetStorageReader(tableName, offset);

            context = new StubContext(configs, offsetReader);

            AmazonDynamoDB dynamoDBClient = Mockito.mock(AmazonDynamoDB.class);
            tableScanner = Mockito.mock(TableScanner.class);

            if (initSyncRecords != null) {
                Answer<ScanResult> ans = invocation -> new ScanResult()
                        .withLastEvaluatedKey(this.exclusiveStartKey)
                        .withItems(this.initSyncRecords);

                when(tableScanner.getItems(ArgumentMatchers.any())).thenAnswer(ans);
            }

            kclWorker = Mockito.mock(KclWorker.class);

            // table description
            if (tableDescription == null) {
                tableDescription = new TableDescription();
                tableDescription.setTableName(tableName);
            }

            DescribeTableResult describeTableResult = new DescribeTableResult();
            describeTableResult.setTable(tableDescription);
            when(dynamoDBClient.describeTable(tableName)).thenReturn(describeTableResult);

            if (clock == null) {
                clock = Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC"));
            }

            // initialize task
            DynamoDBSourceTask task = new DynamoDBSourceTask(clock, dynamoDBClient, tableScanner, kclWorker);

            task.initialize(context);

            if (syncRecords != null) {
                for (KclRecordsWrapper r : syncRecords) {
                    task.getEventsQueue().put(r);
                }
            }

            return task;
        }

        public SourceTaskBuilder withSyncRecords(List<KclRecordsWrapper> syncRecords) {
            this.syncRecords = syncRecords;
            return this;
        }
    }

    private RecordAdapter getRecordAdapter(Map<String, AttributeValue> key,
                                           Map<String, AttributeValue> row,
                                           Instant createDate,
                                           String sequenceNumber,
                                           String operation) {
        StreamRecord streamRecord = new StreamRecord();
        streamRecord.setApproximateCreationDateTime(Date.from(createDate));
        streamRecord.setSequenceNumber(sequenceNumber);
        streamRecord.setKeys(key);
        streamRecord.setNewImage(row);

        com.amazonaws.services.dynamodbv2.model.Record dynamodbRecord =
                new Record();
        dynamodbRecord.setDynamodb(streamRecord);
        dynamodbRecord.setEventName(operation);
        return new RecordAdapter(dynamodbRecord);
    }


    @Test
    public void sourceInfoIsCreatedAndInitSyncStartedOnStartOnThirstRun() throws InterruptedException {
        // Arrange
        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(null) // null means no offset stored for the partition
                .buildTask();

        // Act
        task.start(configs);

        // Assert
        SourceInfo sourceInfo = task.getSourceInfo();
        assertEquals(tableName, sourceInfo.tableName);
        assertEquals(InitSyncStatus.RUNNING, sourceInfo.initSyncStatus);
    }

    @Test
    public void sourceInfoIsSateIsLoadedFromOffsetOnStart() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());
        offset.put("init_sync_end", Instant.parse("2001-01-02T00:00:00.00Z").toEpochMilli());
        offset.put("exclusive_start_key", "{\"t1\":{\"s\":\"v1\"}}");

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .buildTask();

        // Act
        task.start(configs);

        // Assert
        SourceInfo sourceInfo = task.getSourceInfo();
        assertEquals(tableName, sourceInfo.tableName);
        assertEquals(InitSyncStatus.FINISHED, sourceInfo.initSyncStatus);
        assertEquals(Instant.parse("2001-01-01T00:00:00.00Z"), sourceInfo.lastInitSyncStart);
        assertEquals(Instant.parse("2001-01-02T00:00:00.00Z"), sourceInfo.lastInitSyncEnd);
        assertEquals(Collections.singletonMap("t1", new AttributeValue("v1")), sourceInfo.exclusiveStartKey);
    }

    @Test
    public void kclWorkerIsStartedOnStart() throws InterruptedException {
        // Arrange
        SourceTaskBuilder builder = new SourceTaskBuilder();
        DynamoDBSourceTask task = builder.buildTask();

        // Act
        task.start(configs);

        // Assert
        verify(builder.kclWorker, times(1)).start(
                any(AmazonDynamoDB.class),
                any(AmazonDynamoDBStreamsClient.class),
                eq(tableName),
                eq("testTask1"),
                eq(null),
                eq(BillingMode.PROVISIONED)
        );
    }

    @Test
    public void kclWorkerIsStoppedOnStop() throws InterruptedException {
        // Arrange
        SourceTaskBuilder builder = new SourceTaskBuilder();
        DynamoDBSourceTask task = builder.buildTask();

        // Act
        task.start(configs);
        task.stop();

        // Assert
        verify(builder.kclWorker, times(1)).stop();
    }

    @Test
    public void ifTaskIsStoppedPollDoesNothing() throws InterruptedException {
        // Arrange
        DynamoDBSourceTask task = new SourceTaskBuilder().buildTask();

        // Act
        task.start(configs);
        task.stop();
        List<SourceRecord> response = task.poll();

        // Assert
        assertNull(response);
    }


    @Test
    public void onInitSyncRunningPollReturnsScannedItemsBatch() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));
        row.put("col2", new AttributeValue("val1"));
        row.put("col3", new AttributeValue().withN("1"));
        List<Map<String, AttributeValue>> initSyncRecords = Collections.singletonList(row);

        Map<String, AttributeValue> exclusiveStartKey = Collections.singletonMap("fake", new AttributeValue("key"));

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, exclusiveStartKey)
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(Instant.parse("2001-01-01T00:00:00.00Z"), task.getSourceInfo().lastInitSyncStart);
        assertEquals(1, task.getSourceInfo().initSyncCount);

        assertEquals(1, response.size());
        assertEquals("r", ((Struct) response.get(0).value()).getString("op"));
        assertEquals("{\"col2\":{\"s\":\"val1\"},\"col3\":{\"n\":\"1\"},\"col1\":{\"s\":\"key1\"}}", ((Struct) response.get(0).value()).getString("document"));
        assertEquals(InitSyncStatus.RUNNING, task.getSourceInfo().initSyncStatus);
        assertEquals(exclusiveStartKey, task.getSourceInfo().exclusiveStartKey);
    }

    @Test
    public void onStartInitSyncIsDelayed() throws InterruptedException {
        // Arrange
        configs.put("init.sync.delay.period", "2");

        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));
        List<Map<String, AttributeValue>> initSyncRecords = Collections.singletonList(row);

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, null)
                .buildTask();

        // Act
        task.start(configs);
        Instant start = Instant.now();
        List<SourceRecord> response = task.poll();
        Instant stop = Instant.now();

        // Assert
        assertTrue(Duration.between(start, stop).getSeconds() >= 2);
        assertEquals(1, task.getSourceInfo().initSyncCount);
        assertEquals(1, response.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void onInitSyncRunningPollReturnsScannedItemsBatchAndEndsInitSyncIfExclusiveStartKeyIsNull() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());
        offset.put("init_sync_count", 0L);
        offset.put("exclusive_start_key", "{\"t1\":{\"s\":\"v1\"}}");

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row1 = new HashMap<>();
        row1.put("col1", new AttributeValue("key1"));
        Map<String, AttributeValue> row2 = new HashMap<>();
        row2.put("col1", new AttributeValue("key2"));

        List<Map<String, AttributeValue>> initSyncRecords = new LinkedList<>();
        initSyncRecords.add(row1);
        initSyncRecords.add(row2);

        Map<String, AttributeValue> exclusiveStartKey = null;

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, exclusiveStartKey)
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(InitSyncStatus.FINISHED, task.getSourceInfo().initSyncStatus);
        assertEquals(Instant.parse("2001-01-01T00:00:00.00Z"),
                task.getSourceInfo().lastInitSyncStart,
                "Init sync was restarted?");
        assertEquals(Instant.parse("2001-01-01T01:00:00.00Z"), task.getSourceInfo().lastInitSyncEnd);
        assertEquals(2, task.getSourceInfo().initSyncCount);
        assertNull(task.getSourceInfo().exclusiveStartKey);

        assertEquals(2, response.size());
        assertEquals("RUNNING", ((Map<String, String>) response.get(0).sourceOffset()).get("init_sync_state"));
        assertEquals("FINISHED", ((Map<String, String>) response.get(1).sourceOffset()).get("init_sync_state"));

        Struct sourceInfoFromValue1 = ((Struct) response.get(0).value()).getStruct("source");
        assertEquals(true, sourceInfoFromValue1.get("init_sync"));
        assertEquals("RUNNING", sourceInfoFromValue1.get("init_sync_state"));
        Struct sourceInfoFromValue2 = ((Struct) response.get(1).value()).getStruct("source");
        assertEquals(true, sourceInfoFromValue1.get("init_sync"));
        assertEquals("FINISHED", sourceInfoFromValue2.get("init_sync_state"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void onInitSyncOnlyLastRecordHasLatestSourceInfoState() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());
        offset.put("init_sync_count", 0L);
        offset.put("exclusive_start_key", "{\"t1\":{\"s\":\"v1\"}}");

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row1 = new HashMap<>();
        row1.put("col1", new AttributeValue("key1"));
        Map<String, AttributeValue> row2 = new HashMap<>();
        row2.put("col1", new AttributeValue("key2"));

        List<Map<String, AttributeValue>> initSyncRecords = new LinkedList<>();
        initSyncRecords.add(row1);
        initSyncRecords.add(row2);

        Map<String, AttributeValue> exclusiveStartKey = null;

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, exclusiveStartKey)
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(2, response.size());
        assertEquals("RUNNING", ((Map<String, String>) response.get(0).sourceOffset()).get("init_sync_state"));
        assertEquals("{\"t1\":{\"s\":\"v1\"}}", ((Map<String, String>) response.get(0).sourceOffset()).get("exclusive_start_key"));
        assertEquals("FINISHED", ((Map<String, String>) response.get(1).sourceOffset()).get("init_sync_state"));
        assertEquals(null, ((Map<String, String>) response.get(1).sourceOffset()).get("exclusive_start_key"));
    }

    @Test
    public void onInitSyncPollEndsInitSyncIfExclusiveStartKeyIsNullWithNoRecordsReturned() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        List<Map<String, AttributeValue>> initSyncRecords = new LinkedList<>();
        Map<String, AttributeValue> exclusiveStartKey = null; // Means no more data available to scan

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T01:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, exclusiveStartKey)
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(InitSyncStatus.FINISHED, task.getSourceInfo().initSyncStatus);
        assertEquals(Instant.parse("2001-01-01T00:00:00.00Z"),
                task.getSourceInfo().lastInitSyncStart,
                "Init sync was restarted?");
        assertEquals(Instant.parse("2001-01-01T01:00:00.00Z"), task.getSourceInfo().lastInitSyncEnd);
        assertNull(task.getSourceInfo().exclusiveStartKey);

        assertEquals(0, response.size());
    }

    @Test
    public void onInitSyncRunningPollRestartsInitSyncIfTakingToLongToFinish() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "RUNNING");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));
        List<Map<String, AttributeValue>> initSyncRecords = Collections.singletonList(row);

        Map<String, AttributeValue> exclusiveStartKey = null; // Means no more data available to scan

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-01T19:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withInitSyncRecords(initSyncRecords, exclusiveStartKey)
                .buildTask();

        // Act
        task.start(configs);
        task.poll();

        // Assert
        assertEquals(Instant.parse("2001-01-01T19:00:00.00Z"), task.getSourceInfo().lastInitSyncStart);
    }

    @Test
    public void onSyncPollReturnsControlToParentThreadOnRegularIntervalsByReturningNull() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .buildTask();

        // Act & Assert
        task.start(configs);

        assertTimeout(Duration.ofSeconds(2), () -> {
            int returned = 0;
            while (returned < 3) {
                assertNull(task.poll()); // Should return control every 500 milliseconds
                returned++;
            }
        });
    }


    @Test
    public void onSyncPollReturnsReceivedRecords() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));
        row.put("col2", new AttributeValue("val1"));
        row.put("col3", new AttributeValue().withN("1"));

        dynamoDBRecords.getRecords().add(
                getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withS("key1")),
                        row, Instant.parse("2001-01-01T01:00:00.00Z"),
                        "1000000001",
                        "INSERT"));
        dynamoDBRecords.getRecords().add(
                getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withS("key2")),
                        null, Instant.parse("2001-01-01T01:00:00.00Z"),
                        "1000000002",
                        "REMOVE"));

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(3, response.size());
        assertEquals("{\"col2\":{\"s\":\"val1\"},\"col3\":{\"n\":\"1\"},\"col1\":{\"s\":\"key1\"}}", ((Struct) response.get(0).value()).getString("document"));
        assertEquals("{\"col1\":{\"s\":\"key2\"}}", ((Struct) response.get(1).value()).getString("document"));
        assertNull(response.get(2).value());  // tombstone
    }

    @Test
    public void onSyncPollReturnsReceivedRecordsWithCorrectOperationChosen() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));

        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"), "1000000001",
                "INSERT"));
        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"), "1000000002",
                "MODIFY"));
        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"), "1000000003",
                "REMOVE"));


        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals("c", ((Struct) response.get(0).value()).getString("op"));
        assertEquals("u", ((Struct) response.get(1).value()).getString("op"));
        assertEquals("d", ((Struct) response.get(2).value()).getString("op"));
    }

    @Test
    public void onSyncPollReturnsDeletedRecordAndTombstoneMessage() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));

        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"), "1000000003",
                "REMOVE"));


        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals("d", ((Struct) response.get(0).value()).getString("op"));
        assertNull(response.get(1).value()); // tombstone message
    }

    @Test
    public void onSyncPollSkipsRecordsWhichHappenedBeforeTheLastInitSync() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));

        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"), "1000000001",
                "INSERT"));
        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2000-12-12T23:01:00.00Z"), "1000000002",
                "INSERT"));

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        ConcurrentHashMap<String, ShardInfo> shardRegister = task.getShardRegister();
        shardRegister.put("testShardId1", new ShardInfo("testShardId1"));

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(1, response.size());
        assertEquals("1000000001", response.get(0).sourceOffset().get("src_shard_sequence_no"));

        assertEquals("1000000002", shardRegister.get("testShardId1").getLastCommittedRecordSeqNo(), "SeqNo for skipped " +
                "records must be registered");
    }


    @Test
    public void onSyncPollLogsErrorAndDropsRecordsWhichCannotBeParsed() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));

        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"),
                "10000000000000000000001",
                "INSERT"));
        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-01T01:00:00.00Z"),
                "10000000000000000000002"
                , "INVALID"));

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        ConcurrentHashMap<String, ShardInfo> shardRegister = task.getShardRegister();
        shardRegister.put("testShardId1", new ShardInfo("testShardId1"));

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertEquals(1, response.size());
        assertEquals("10000000000000000000001", response.get(0).sourceOffset().get("src_shard_sequence_no"));
        assertEquals("10000000000000000000002", shardRegister.get("testShardId1").getLastCommittedRecordSeqNo(), "SeqNo " +
                "for " +
                "dropped events must be registered");
    }

    @Test
    public void onSyncPollReturnsNullAndStartsInitSyncIfAnyOneRecordEventArrivedToLateAndAfterLastInitSyncStart() throws InterruptedException {
        // Arrange
        HashMap<String, Object> offset = new HashMap<>();
        offset.put("table_name", tableName);
        offset.put("init_sync_state", "FINISHED");
        offset.put("init_sync_start", Instant.parse("2001-01-01T00:00:00.00Z").toEpochMilli());

        KclRecordsWrapper dynamoDBRecords = new KclRecordsWrapper("testShardId1", new LinkedList<>());

        TableDescription tableDescription = new TableDescription();
        tableDescription.setTableName(tableName);
        tableDescription.setKeySchema(Collections.singleton(new KeySchemaElement("col1", "S")));

        Map<String, AttributeValue> row = new HashMap<>();
        row.put("col1", new AttributeValue("key1"));

        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-03T15:00:00.00Z"), "s1", "INSERT"));
        dynamoDBRecords.getRecords().add(getRecordAdapter(Collections.singletonMap("col1", new AttributeValue().withN("key1")),
                row, Instant.parse("2001-01-03T00:00:00.00Z"), "s2", "INSERT"));

        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(offset)
                .withClock(Clock.fixed(Instant.parse("2001-01-03T20:00:00.00Z"), ZoneId.of("UTC")))
                .withTableDescription(tableDescription)
                .withSyncRecords(Collections.singletonList(dynamoDBRecords))
                .buildTask();

        // Act
        task.start(configs);
        List<SourceRecord> response = task.poll();

        // Assert
        assertNull(response);
        assertEquals(InitSyncStatus.RUNNING, task.getSourceInfo().initSyncStatus);
    }

    @Test
    public void onCommitRecordShardRegisterIsUpdatedForEachShardWithLatestRecordSequenceCommittedToKafka() throws InterruptedException {
        // Arrange
        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(null) // null means no offset stored for the partition
                .buildTask();

        ConcurrentHashMap<String, ShardInfo> shardRegister = task.getShardRegister();
        shardRegister.put("shard1", new ShardInfo("shard1"));

        HashMap<String, Object> offset = new HashMap<>();
        offset.put("src_shard_id", "shard1");
        offset.put("src_shard_sequence_no", "1000000000000000000001");

        SourceRecord sourceRecord = new SourceRecord(
                null,
                offset,
                "TestTopicName",
                null, null
        );

        // Act
        task.start(configs);
        task.commitRecord(sourceRecord);

        // Assert
        assertEquals("1000000000000000000001", shardRegister.get("shard1").getLastCommittedRecordSeqNo());
    }

    @Test
    public void latestSequenceNumberIsRegistered() throws InterruptedException {
        // Arrange
        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(null) // null means no offset stored for the partition
                .buildTask();

        ConcurrentHashMap<String, ShardInfo> shardRegister = task.getShardRegister();
        shardRegister.put("shard1", new ShardInfo("shard1"));

        HashMap<String, Object> offset1 = new HashMap<>();
        offset1.put("src_shard_id", "shard1");
        offset1.put("src_shard_sequence_no", "9999900000000011822709669");

        SourceRecord sourceRecord1 = new SourceRecord(
                null,
                offset1,
                "TestTopicName",
                null, null
        );

        HashMap<String, Object> offset2 = new HashMap<>();
        offset2.put("src_shard_id", "shard1");
        offset2.put("src_shard_sequence_no", "10044100000000011822719112");

        SourceRecord sourceRecord2 = new SourceRecord(
                null,
                offset2,
                "TestTopicName",
                null, null
        );

        // Act
        task.start(configs);
        task.commitRecord(sourceRecord1);
        task.commitRecord(sourceRecord2);

        // Assert
        assertEquals("10044100000000011822719112", shardRegister.get("shard1").getLastCommittedRecordSeqNo());
    }

    @Test
    public void onCommitIgnoreRecordsWithoutSequenceNumber() throws InterruptedException {
        // Arrange
        DynamoDBSourceTask task = new SourceTaskBuilder()
                .withOffset(null) // null means no offset stored for the partition
                .buildTask();

        ConcurrentHashMap<String, ShardInfo> shardRegister = task.getShardRegister();
        shardRegister.put("shard1", new ShardInfo("shard1"));

        HashMap<String, Object> offset = new HashMap<>();
        offset.put("src_shard_id", "initSyncStatus");

        SourceRecord sourceRecord = new SourceRecord(
                null,
                offset,
                "TestTopicName",
                null, null
        );

        // Act
        task.start(configs);
        task.commitRecord(sourceRecord);

        // Assert
        assertEquals("", shardRegister.get("shard1").getLastCommittedRecordSeqNo());
    }

}
