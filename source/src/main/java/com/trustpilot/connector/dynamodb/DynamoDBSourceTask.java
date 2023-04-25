package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.model.Record;
import com.trustpilot.connector.dynamodb.aws.AwsClients;
import com.trustpilot.connector.dynamodb.aws.DynamoDBTableScanner;
import com.trustpilot.connector.dynamodb.aws.TableScanner;
import com.trustpilot.connector.dynamodb.kcl.KclRecordsWrapper;
import com.trustpilot.connector.dynamodb.kcl.KclWorker;
import com.trustpilot.connector.dynamodb.kcl.KclWorkerImpl;
import com.trustpilot.connector.dynamodb.kcl.ShardInfo;
import com.trustpilot.connector.dynamodb.utils.RecordConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This source tasks tracks all DynamoDB table changes via DynamoDB Streams.
 * (Table must have Streams enabled with `new_image` mode)
 * <p>
 * Tracking DynamoDB Streams(which are similar to Kinesis Streams) is quite complicated
 * therefor Kinesis Client Library(KCL) + DynamoDB Streams Adapter is used.
 * <p>
 * Short workflow description:
 * <p>
 * - On Start this task:
 * a. Initiates KCL worker on a separate thread. This worker tracks
 * DynamoDB Stream shards and returns received events back to this task via ArrayBlockingQueue.
 * If queue is full KCL worker blocks and waits indefinitely for it to empty.
 * <p>
 * b. Tries to load current source state({@link SourceInfo}) from Kafka Connect offsets.
 * <p>
 * c. If there is no state saved or if saved state indicates unfinished InitSync tasks goes into InitSync mode.
 * <p>
 * - On Poll(normal task execution):
 * a. If running in InitSync mode, scans next batch from source table until all data is fetched. When goes into
 * InitSync finished mode.
 * <p>
 * b. If InitSync mode is finished awaits data from KCL worker via ArrayBlockingQueue.
 * Once data is received thirst validates that each item is:
 * 1. If item was created after (InitSync.StartTime-1h). This check is done primarily to limit amount of
 * duplicates
 * sent to Kafka. And secondly to "fast foward" source stream into safe to consume items range.
 * <p>
 * 2. If item was created after (InitSync.StartTime-1h) but is already behind (Time.Now-20h) it is assumed,
 * that we might have missed some of the records and source tasks goes into InitSync mode.
 * If both validations pass items data is converted into Kafka Connect format and pushed to Kafka.
 * <p>
 * - On Commit record (which is called for each record pushed to Kafka topic):
 * Updates special {@see #shardRegister} object with latest KCL shard's sequence number from item which was just
 * pushed into Kafka. KCL worker uses this {@see #shardRegister} to track it's own progress in reading source
 * tables stream. In case of unexpected(or expected) shutdown KCL worker will use this checkpointed sequence
 * number to continue reading data.
 * Because KCL worker checkpoints at regular time intervals as instead of after each record. Some data can be
 * duplicated after recovery or restarts.
 * <p>
 * <p>
 * NOTE: Currently only one task per tableName is supported!
 */
public class DynamoDBSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBSourceTask.class);
    private Clock clock = Clock.systemUTC();

    private final ArrayBlockingQueue<KclRecordsWrapper> eventsQueue = new ArrayBlockingQueue<>(10, true);

    private AmazonDynamoDB client;
    private TableScanner tableScanner;
    private RecordConverter converter;
    private volatile KclWorker kclWorker;

    private volatile Boolean shutdown;

    private final ConcurrentHashMap<String, ShardInfo> shardRegister = new ConcurrentHashMap<>();
    private SourceInfo sourceInfo;
    private TableDescription tableDesc;
    private int initSyncDelay;
    private boolean initSyncDisable;

    @SuppressWarnings("unused")
    //Used by Confluent platform to initialize connector
    public DynamoDBSourceTask() {
    }

    DynamoDBSourceTask(Clock clock, AmazonDynamoDB client, TableScanner tableScanner, KclWorker kclWorker) {
        this.clock = clock;
        this.client = client;
        this.tableScanner = tableScanner;
        this.kclWorker = kclWorker;
    }

    @Override
    public String version() {
        return "0.1";
    }

    public void start(Map<String, String> configProperties) {

        DynamoDBSourceTaskConfig config = new DynamoDBSourceTaskConfig(configProperties);
        LOGGER.info("Starting task for table: {}", config.getTableName());

        LOGGER.debug("Getting DynamoDB description for table: {}", config.getTableName());
        if (client == null) {
            client = AwsClients.buildDynamoDbClient(
                    config.getAwsRegion(),
                    config.getDynamoDBServiceEndpoint(),
                    config.getAwsAccessKeyIdValue(),
                    config.getAwsSecretKeyValue(),
                    config.getAwsAssumeRoleArn());
        }
        tableDesc = client.describeTable(config.getTableName()).getTable();

        initSyncDelay = config.getInitSyncDelay();
        initSyncDisable = config.getInitSyncDisable();

        LOGGER.debug("Getting offset for table: {}", tableDesc.getTableName());
        setStateFromOffset();
        LOGGER.info("Task status: {}", sourceInfo);

        LOGGER.debug("Initiating DynamoDB table scanner and record converter.");
        if (tableScanner == null) {
            tableScanner = new DynamoDBTableScanner(client,
                    tableDesc.getTableName(),
                    tableDesc.getProvisionedThroughput().getReadCapacityUnits());
        }
        converter = new RecordConverter(tableDesc, config.getDestinationTopicPrefix());

        LOGGER.info("Starting background KCL worker thread for table: {}", tableDesc.getTableName());

        AmazonDynamoDBStreams dynamoDBStreamsClient =  AwsClients.buildDynamoDbStreamsClient(
                config.getAwsRegion(),
                config.getDynamoDBServiceEndpoint(),
                config.getAwsAccessKeyIdValue(),
                config.getAwsSecretKeyValue(),
                config.getAwsAssumeRoleArn());

        if (kclWorker == null) {
            kclWorker = new KclWorkerImpl(  // always start from TRIM_HORIZON
                    AwsClients.getCredentials(config.getAwsAccessKeyIdValue(), config.getAwsSecretKeyValue(), config.getAwsAssumeRoleArn()),
                    eventsQueue,
                    shardRegister);
        }
        kclWorker.start(client, dynamoDBStreamsClient, tableDesc.getTableName(), config.getTaskID(), config.getDynamoDBServiceEndpoint(), config.getKCLTableBillingMode());

        shutdown = false;
    }

    private void setStateFromOffset() {
        Map<String, Object> offset = context.offsetStorageReader()
                                            .offset(Collections.singletonMap("table_name", tableDesc.getTableName()));
        if (offset != null) {
            sourceInfo = SourceInfo.fromOffset(offset, clock);
        } else {
            LOGGER.debug("No stored offset found for table: {}", tableDesc.getTableName());
            sourceInfo = new SourceInfo(tableDesc.getTableName(), clock);            
            if (initSyncDisable) {
                sourceInfo.endInitSync();
                sourceInfo.initSync = false;
                LOGGER.info("INIT_SYNC disabled, sourceInfo initSyncStatus={}, lastInitSyncStart={}, lastInitSyncEnd={}",  
                    sourceInfo.initSyncStatus,
                    sourceInfo.lastInitSyncStart,
                    sourceInfo.lastInitSyncEnd
                );
            } else {
                sourceInfo.startInitSync(); // InitSyncStatus always needs to run after adding new table
                LOGGER.info("INIT_SYNC enabled, sourceInfo initSyncStatus={}, lastInitSyncStart={}, lastInitSyncEnd={}",  
                    sourceInfo.initSyncStatus,
                    sourceInfo.lastInitSyncStart,
                    sourceInfo.lastInitSyncEnd
                );
            }
        }
    }

    @Override
    public synchronized void stop() {
        shutdown = true;
        LOGGER.info("Stoping background KCL worker thread for table: {}", tableDesc.getTableName());
        kclWorker.stop();
    }

    /**
     * Depending on current source task state executed either INIT_SYNC or SYNC operation.
     * <p>
     * `poll` method should not hold execution for long intervals therefore INIT_SYNC executes
     * in batches on each `poll` execution and SYNC returns control regularly even if there is
     * no new data.
     */
    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            if (shutdown) {
                LOGGER.debug("Poll exits because shutdown was requested");
                return null;
            }
            if (sourceInfo.initSyncStatus == InitSyncStatus.RUNNING) {
                return initSync();
            }
            if (sourceInfo.initSyncStatus == InitSyncStatus.FINISHED) {
                return sync();
            }
            throw new Exception("Invalid SourceInfo InitSyncStatus state: " + sourceInfo.initSyncStatus);
        } catch (InterruptedException ex) {
            LOGGER.error("Failed to handle incoming records. Records dropped!", ex);
            throw ex;
        } catch (Exception ex) {
            LOGGER.error("Failed to handle incoming records. Records dropped!", ex);
        }
        return null;
    }

    /**
     * Scans source table in batches.
     * <p>
     * Actual `iteration` is executed by framework calls to `poll` and storing last read source item in
     * {@link SourceInfo}.
     */
    private LinkedList<SourceRecord> initSync() throws Exception {
        if (sourceInfo.lastInitSyncStart.compareTo(Instant.now(clock).minus(Duration.ofHours(19))) <= 0) {
            LOGGER.error("Current INIT_SYNC took over 19 hours. Restarting INIT_SYNC! {}", sourceInfo);
            sourceInfo.startInitSync();
        }

        sourceInfo.initSync = true;

        if (sourceInfo.initSyncCount == 0) {
            LOGGER.warn("Delaying init sync start by {} sec. This gives Connect cluster time to calm down. {}",
                        initSyncDelay * 1000,
                        sourceInfo);
            Thread.sleep(initSyncDelay * 1000);
        }

        LOGGER.info("Continuing INIT_SYNC {}", sourceInfo);
        ScanResult scanResult = tableScanner.getItems(sourceInfo.exclusiveStartKey);

        LinkedList<SourceRecord> result = new LinkedList<>();
        Map<String, AttributeValue> lastRecord = null;
        for (Map<String, AttributeValue> record : scanResult.getItems()) {
            lastRecord = record;
            sourceInfo.initSyncCount = sourceInfo.initSyncCount + 1;
            result.add(converter.toSourceRecord(sourceInfo,
                                                Envelope.Operation.READ,
                                                record,
                                                sourceInfo.lastInitSyncStart,
                                                null,
                                                null));
        }

        // Only update exclusiveStartKey and init sync state on last record.
        // Otherwise we could loose some data in case of a crash when not all records from this batch are committed.
        sourceInfo.exclusiveStartKey = scanResult.getLastEvaluatedKey();
        if (sourceInfo.exclusiveStartKey == null) {
            sourceInfo.endInitSync();
        }

        // Add last record with updated state info
        if (!result.isEmpty()) {
            result.removeLast();
            result.add(converter.toSourceRecord(sourceInfo,
                                                Envelope.Operation.READ,
                                                lastRecord,
                                                sourceInfo.lastInitSyncStart,
                                                null,
                                                null));
        }

        if (sourceInfo.initSyncStatus == InitSyncStatus.RUNNING) {
            LOGGER.info(
                    "INIT_SYNC iteration returned {}. Status: {}", result.size(), sourceInfo);
        } else {
            LOGGER.info("INIT_SYNC FINISHED: {}", sourceInfo);
        }
        return result;
    }

    /**
     * Returns new events from queue. If there is no new data returns control to Connect framework regularly by
     * returning null.
     */
    private List<SourceRecord> sync() throws Exception {
        LOGGER.debug("Waiting for records from eventsQueue for table: {}", tableDesc.getTableName());
        KclRecordsWrapper dynamoDBRecords = eventsQueue.poll(500, TimeUnit.MILLISECONDS);
        if (dynamoDBRecords == null) {
            return null; // returning thread control at regular intervals
        }

        sourceInfo.initSync = false;

        LOGGER.debug("Records({}) received from shard: {} for table: {}", dynamoDBRecords.getRecords().size(),
                     dynamoDBRecords.getShardId(),
                     tableDesc.getTableName());
        List<SourceRecord> result = new LinkedList<>();
        for (Record record : dynamoDBRecords.getRecords()) {
            try {
                Date arrivalTimestamp = record.getApproximateArrivalTimestamp();

                // Ignoring records created before last init sync.
                // NOTE1: This should happen only after init sync, during catchup.
                // After catchup newest record should still be within accepted safe interval or init sync will start again.
                //
                // NOTE2: KCL worker reads from multiple shards at the same time in a loop.
                // Which means that there can be messages from various time instances (before and after init sync start instance).
                if (isPreInitSyncRecord(arrivalTimestamp)) {
                    LOGGER.debug(
                            "Dropping old record to prevent another INIT_SYNC. ShardId: {} " +
                                    "ApproximateArrivalTimestamp: {} CurrentTime: {}",
                            dynamoDBRecords.getShardId(),
                            arrivalTimestamp.toInstant(),
                            Instant.now(clock));

                    // We still need to mark those records as committed or KCL work won't be able to proceed
                    RegisterAsProcessed(dynamoDBRecords.getShardId(), record.getSequenceNumber());
                    continue;
                }

                // Received record which is behind "safe" zone. Indicating that "potentially" we lost some records.
                // Need to resync...
                // This happens if:
                // * connector was down for some time
                // * connector is lagging
                // * connector failed to finish init sync in acceptable time frame
                if ( (!initSyncDisable) && recordIsInDangerZone(arrivalTimestamp)) {
                    sourceInfo.startInitSync();

                    LOGGER.info(
                            "Received record which is lagging > 20 hours.. Starting INIT_SYNC!! ShardId: {} " +
                                    "CurrentTime: {} " +
                                    "ApproximateArrivalTimestamp: " +
                                    "{}",
                            dynamoDBRecords.getShardId(),
                            Instant.now(clock),
                            arrivalTimestamp.toInstant());

                    return null;
                }

                com.amazonaws.services.dynamodbv2.model.Record dynamoDbRecord =
                        ((RecordAdapter) record).getInternalObject();

                Envelope.Operation op = getOperation(dynamoDbRecord.getEventName());

                Map<String, AttributeValue> attributes;
                if (dynamoDbRecord.getDynamodb().getNewImage() != null) {
                    attributes = dynamoDbRecord.getDynamodb().getNewImage();
                } else {
                    attributes = dynamoDbRecord.getDynamodb().getKeys();
                }

                SourceRecord sourceRecord = converter.toSourceRecord(sourceInfo,
                                                                     op,
                                                                     attributes,
                                                                     arrivalTimestamp.toInstant(),
                                                                     dynamoDBRecords.getShardId(),
                                                                     record.getSequenceNumber());
                result.add(sourceRecord);

                if (op == Envelope.Operation.DELETE) {
                    // send a tombstone event (null value) for the old key so it can be removed from the Kafka log eventually...
                    SourceRecord tombstoneRecord = new SourceRecord(sourceRecord.sourcePartition(),
                                                                    sourceRecord.sourceOffset(),
                                                                    sourceRecord.topic(),
                                                                    sourceRecord.keySchema(), sourceRecord.key(),
                                                                    null, null);
                    result.add(tombstoneRecord);
                }

            } catch (Exception ex) {
                LOGGER.error("Failed to parse record. Dropping and proceeding with the next one.", ex); //
                // Fake commit. Allow KCL worker to proceed with checkpoiting and reading next shard
                RegisterAsProcessed(dynamoDBRecords.getShardId(), record.getSequenceNumber());
            }
        }

        return result;
    }

    private boolean isPreInitSyncRecord(Date arrivalTimestamp) {
        return arrivalTimestamp.toInstant()
                               .plus(Duration.ofHours(1))
                               .compareTo(sourceInfo.lastInitSyncStart) <= 0;
    }

    private boolean recordIsInDangerZone(Date arrivalTimestamp) {
        return arrivalTimestamp.toInstant().compareTo(Instant.now(clock).minus(Duration.ofHours(20))) <= 0;
    }

    private Envelope.Operation getOperation(String eventName) throws Exception {
        switch (eventName) {
            case "INSERT":
                return Envelope.Operation.CREATE;
            case "MODIFY":
                return Envelope.Operation.UPDATE;
            case "REMOVE":
                return Envelope.Operation.DELETE;
            default:
                throw new Exception("Unsupported DynamoDB event name: " + eventName);
        }
    }

    /**
     * Tracks sequence numbers of all events which have been pushed to Kafka.
     * <p>
     * KCL worker depends on this to checkpoint only up to pushed events.
     */
    @Override
    public void commitRecord(SourceRecord record) {
        if (record.sourceOffset().containsKey(RecordConverter.SHARD_SEQUENCE_NO)) {
            String shardId = (String) record.sourceOffset().get(RecordConverter.SHARD_ID);
            String sequenceNumber = (String) record.sourceOffset().get(RecordConverter.SHARD_SEQUENCE_NO);

            if (sequenceNumber != null && !sequenceNumber.isEmpty()) {
                RegisterAsProcessed(shardId, sequenceNumber);
            }
        }
    }

    private void RegisterAsProcessed(String shardId, String sequenceNumber) {
        ShardInfo shardInfo = shardRegister.get(shardId);
        String currentSeqNo = shardInfo.getLastCommittedRecordSeqNo();

        // Prevent issues with out of order registration.
        // Since we "commit" some records before sending to Kafka(and them being committed) in some cases.
        if (currentSeqNo != null && !currentSeqNo.equals("")) {
            BigInteger currentSeqNoInt = new BigInteger(shardInfo.getLastCommittedRecordSeqNo());
            BigInteger sequenceNumberInt = new BigInteger(sequenceNumber);

            if (currentSeqNoInt.compareTo(sequenceNumberInt) >= 0) {
                return;
            }
        }

        LOGGER.debug("commitRecord: ShardID: {} lastPushedSequenceNumber: {}", shardId, sequenceNumber);
        shardInfo.setLastCommittedRecordSeqNo(sequenceNumber);
    }

    ConcurrentHashMap<String, ShardInfo> getShardRegister() {
        return shardRegister;
    }

    ArrayBlockingQueue<KclRecordsWrapper> getEventsQueue() {
        return eventsQueue;
    }

    SourceInfo getSourceInfo() {
        return sourceInfo;
    }
}