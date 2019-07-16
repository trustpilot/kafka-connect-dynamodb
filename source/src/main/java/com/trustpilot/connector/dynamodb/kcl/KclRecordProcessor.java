package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.trustpilot.connector.dynamodb.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * KclRecordProcessor receives records from DynamoDB Stream shards and passes them over to
 * {@link com.trustpilot.connector.dynamodb.DynamoDBSourceTask}
 * It is also checkpoints it's progress at regular intervals or on shutdowns.
 *
 * NOTE: KclWorker manages KclRecordProcessor's dynamically as needed. One record processor is
 * created for each DynamoDB table Streams shard  current worker is consuming.
 * And will be shutdown(terminated) once end of the shard is reached.
 *
 * NOTE: There can be multiple KclRecordProcessors consuming different shards at the same time.
 *
 * NOTE: KclWorkerImpl runs on one thread and all KclRecordProcessor are invoked in order, one after another.
 *
 * While checkpoiting KclRecordProcessor depends on {@link KclRecordProcessor#shardRegister}
 * to know which records have already been committed to Kafka. Each {@link KclRecordProcessor}
 * instance registers and unregisters itself to this ConcurrentHashMap.
 *
 * NOTE: Since checkpoints are saved at regular intervals and not after each delivered event there can be
 * duplicate events delivered in case of unclean shutdown recovery.
 *
 */
public class KclRecordProcessor implements IRecordProcessor, IShutdownNotificationAware {
    private static final Logger LOGGER = LoggerFactory.getLogger(KclRecordProcessor.class);
    private Clock clock;

    private final String tableName;
    private final ArrayBlockingQueue<KclRecordsWrapper> eventsQueue;

    /**
     * Shared shard register between each instance of record processor(one for each shard)
     * and {@link KclRecordsWrapper}.
     * Used to track and synchronize which records have been committed to Kafka. This enables
     * recovering from checkpoints without loosing data.
     */
    private final ConcurrentHashMap<String, ShardInfo> shardRegister;
    private String shardId;
    private long lastCheckpointTime;

    /**
     * Sequence number of last record which was received from Stream and pushed to {@link #eventsQueue}.
     *
     * Used to track if all "processed" records from this shard have been committed to Kafka
     * before moving on to next shard. (This {@link KclRecordProcessor} instance terminates at shard end.)
     */
    private String lastProcessedSeqNo;

    private Boolean shutdownRequested = false;


    public KclRecordProcessor(String tableName,
                              ArrayBlockingQueue<KclRecordsWrapper> dataQueue,
                              ConcurrentHashMap<String, ShardInfo> shardRegister,
                              Clock clock) {
        this.tableName = tableName;
        this.eventsQueue = dataQueue;
        this.shardRegister = shardRegister;
        this.clock = clock;
    }


    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();
        lastCheckpointTime = clock.millis();
        lastProcessedSeqNo = "";

        LOGGER.debug("Registering new recordProcessor for ShardId: {}", shardId);
        shardRegister.putIfAbsent(shardId, new ShardInfo(initializationInput.getShardId()));
    }

    /**
     * Worker is configured to invoke {@link KclRecordProcessor} even if there is no new event records.
     *
     * This allows checkpoints to execute at regular intervals.
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        if (processRecordsInput.getRecords() != null && processRecordsInput.getRecords().size() > 0) {
            process(processRecordsInput);
        }
        checkpoint(processRecordsInput.getCheckpointer());
    }

    /**
     * Pushes records into {@link #eventsQueue}. Waits indefinitely if queue is full.
     *
     */
    private void process(ProcessRecordsInput processRecordsInput) {
        List<Record> records = processRecordsInput.getRecords();
        LOGGER.debug("Received records Table: {} ShardID: {} Count: {}", tableName, shardId, records.size());

        KclRecordsWrapper events = new KclRecordsWrapper(shardId, records);

        try {
            boolean added = false;
            while (!added && !shutdownRequested){
                LOGGER.debug("KclRecordProcessor {} putting events into queue.", this.shardId);
                added = eventsQueue.offer(events, 100, TimeUnit.MILLISECONDS);
            }

            if (shutdownRequested) {
                LOGGER.warn("KclRecordProcessor {} 'process' method canceled because shutdown was requested", this.shardId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.info("Received interrupt. While trying to put records into queue. Quiting.", e);
            return;
        }

        String firstProcessedSeqNo = records.get(0).getSequenceNumber();
        lastProcessedSeqNo = records.get(records.size() - 1).getSequenceNumber();
        LOGGER.info("Added {} records to eventsQueue. Table: {} ShardID: {}, FirstSeqNo: {}, LastSeqNo: {}",
                    records.size(),
                    tableName,
                    shardId,
                    firstProcessedSeqNo,
                    lastProcessedSeqNo);
    }

    /**
     * Checkpoints shard read progress at regular intervals.
     * <p>
     * IMPORTANT: it tracks(checkpoints) actual records delivered(committed) to Kafka
     * instead of records which have been consumed by processRecords
     *
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        if (isTimeToCheckpoint()) {
            String lastCommittedRecordSequenceNumber = shardRegister.get(shardId).getLastCommittedRecordSeqNo();

            if (!lastCommittedRecordSequenceNumber.equals("")) {  // If at least one record was committed to Kafka
                try {
                    LOGGER.info("KCL checkpoint table: {} shardId: {} at sequenceNumber: {}",
                                tableName,
                                shardId,
                                lastCommittedRecordSequenceNumber);
                    checkpointer.checkpoint(lastCommittedRecordSequenceNumber);

                    lastCheckpointTime = clock.millis();
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Invalid sequence number", e);
                } catch (InvalidStateException e) {
                    throw new RuntimeException("Invalid kcl state", e);
                } catch (ShutdownException e) {
                    throw new RuntimeException("Failed to checkpoint", e);
                }
            }
        }
    }

    private boolean isTimeToCheckpoint() {
        long passedTime = clock.millis() - lastCheckpointTime;
        return TimeUnit.MILLISECONDS.toSeconds(passedTime) >= Constants.KCL_RECORD_PROCESSOR_CHECKPOINTING_INTERVAL;
    }

    /**
     * Called by KclWorkerImpl whenever it wants to stop processing of this shard.
     *
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        shutdownRequested = true;
        LOGGER.info("KclRecordProcessor {} shutdown requested: {}", this.shardId, shutdownInput.getShutdownReason());

        try {
            // Shard end
            if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
                onTerminate(shutdownInput);
            }

            // Some other worker has taken over processing of this shard?
            if (shutdownInput.getShutdownReason() == ShutdownReason.ZOMBIE) {
                onZombie();
            }
        } catch (InterruptedException e) {
            LOGGER.info("Thread interrupted during shutdown", e);
            Thread.currentThread().interrupt();
        } catch (InvalidStateException e) {
            throw new RuntimeException("Invalid kcl state", e);
        } catch (ShutdownException e) {
            throw new RuntimeException("Failed to checkpoint", e);
        }
    }

    /**
     * Called on ShutdownReason.TERMINATE
     */
    private void onTerminate(ShutdownInput shutdownInput) throws InvalidStateException, ShutdownException, InterruptedException {
        if (lastProcessedSeqNo != null && !lastProcessedSeqNo.isEmpty()) {
            ShardInfo processorRegister = shardRegister.getOrDefault(this.shardId, null);
            if (processorRegister != null) {
                int i = 0;
                while (!processorRegister.getLastCommittedRecordSeqNo().equals(this.lastProcessedSeqNo)) {
                    if (i % 20 == 0) {
                        LOGGER.info(
                                "Shard ended. Waiting for all data table: {} from shard: {} to be committed. " +
                                        "lastCommittedRecordSeqNo: {} lastProcessedSeqNo: {}",
                                tableName,
                                shardId,
                                processorRegister.getLastCommittedRecordSeqNo(),
                                this.lastProcessedSeqNo);
                    }
                    i += 1;

                    Thread.sleep(500);
                }
            }
        }

        LOGGER.debug("Shard end reached. Removing processor from register. Table: {} ShardID: {}", tableName,
                shardId);
        shardRegister.remove(shardId);

        LOGGER.info(
                "Shard ended. All data committed. Checkpoint and proceed to next one. Table: {} ShardID: {}",
                tableName,
                shardId);
        IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();
        if (checkpointer != null) {
            checkpointer.checkpoint();
        }
    }

    /**
     * Called on ShutdownReason.ZOMBIE
     */
    private void onZombie() {
        LOGGER.warn("Shard record processor terminated as ZOMBIE. Removing processor from register. " +
                "ShardID: {}", shardId);
        shardRegister.remove(shardId);
    }

    /**
     * Executed during graceful shutdown. Allows workers to checkpoint progress before stopping.
     *
     * @param checkpointer
     */
    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        shutdownRequested = true;
        LOGGER.info("Shutdown requested for RecordProcessor table: {} shard: {}", this.tableName, this.shardId);

        ShardInfo shardInfo = shardRegister.get(this.shardId);
        if (!shardInfo.getLastCommittedRecordSeqNo().equals("")) {
            LOGGER.info(
                    "Graceful KCL worker shutdown requested. Checkpointing and ending consumption. Table: {} ShardId: {} SeqNo: {}",
                    this.tableName,
                    this.shardId,
                    shardInfo.getLastCommittedRecordSeqNo());
            try {
                checkpointer.checkpoint(shardInfo.getLastCommittedRecordSeqNo());
            } catch (InvalidStateException | ShutdownException e) {
                LOGGER.error("Failed to checkpoint at shutdown", e);
            }
            shardRegister.remove(shardId);
        }
    }
}