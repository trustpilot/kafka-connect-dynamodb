package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.trustpilot.connector.dynamodb.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Configures Kinesis Client Library worker and starts it on a dedicated thread
 */
public class KclWorkerImpl implements KclWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(KclWorkerImpl.class);

    private final AWSCredentialsProvider awsCredentialsProvider;
    private final ArrayBlockingQueue<KclRecordsWrapper> eventsQueue;
    private final ConcurrentHashMap<String, ShardInfo> recordProcessorsRegister;

    private volatile Thread thread;
    private volatile Worker worker;

    public KclWorkerImpl(AWSCredentialsProvider awsCredentialsProvider,
                         ArrayBlockingQueue<KclRecordsWrapper> eventsQueue,
                         ConcurrentHashMap<String, ShardInfo> recordProcessorsRegister) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.eventsQueue = eventsQueue;
        this.recordProcessorsRegister = recordProcessorsRegister;
    }


    @Override
    public void start(AmazonDynamoDB dynamoDBClient,
                      AmazonDynamoDBStreams dynamoDBStreamsClient,
                      String tableName,
                      String taskid,
                      String endpoint,
                      BillingMode kclTableBillingMode) {
        IRecordProcessorFactory recordProcessorFactory = new KclRecordProcessorFactory(tableName, eventsQueue,
                recordProcessorsRegister);

        KinesisClientLibConfiguration clientLibConfiguration = getClientLibConfiguration(tableName,
                taskid,
                dynamoDBClient, endpoint, kclTableBillingMode);

        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreamsClient);

        // If enabled, throws exception if trying to consume expired shards. But seems there is no way to catch
        // actual exception in KclRecordProcessor
        // adapterClient.setSkipRecordsBehavior(AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY);

        // Only needed for statistics in Cloudwatch. Or if you want to get events as bytes.
        adapterClient.setGenerateRecordBytes(false);

        // Do not use CloudWatch
        AmazonCloudWatch cloudWatchClient = new KclNoopCloudWatch();

        worker = StreamsWorkerFactory
                .createDynamoDbStreamsWorker(recordProcessorFactory,
                        clientLibConfiguration,
                        adapterClient,
                        dynamoDBClient,
                        cloudWatchClient);


        LOGGER.info("Creating KCL worker for Stream: {} ApplicationName: {} WorkerId: {}",
                clientLibConfiguration.getStreamName(),
                clientLibConfiguration.getApplicationName(),
                clientLibConfiguration.getWorkerIdentifier()
        );

        thread = new Thread(worker);
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Attempts graceful shutdown of KCL worker.
     * <p>
     * NOTE: Will be called from different thread on DynamoDbSourceTask.
     * Therefor must be thread safe!
     */
    @Override
    public synchronized void stop() {
        boolean workerStopped = false;
        Future<Boolean> workerShutdownFuture = worker.startGracefulShutdown();
        try {
            workerStopped = workerShutdownFuture.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("Graceful KCL worker stop failed: " + e, e);
        }

        if (workerStopped) {
            LOGGER.info("KCL worker stopped");
        } else {
            LOGGER.info("Force stop KCL worker");
            worker.shutdown();
        }

        try {
            thread.join(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Failed to stop kcl_worker thread: " + e, e);
        }
    }

    KinesisClientLibConfiguration getClientLibConfiguration(String tableName,
                                                            String taskid,
                                                            AmazonDynamoDB dynamoDBClient,
                                                            String endpoint,
                                                            BillingMode kclTableBillingMode) {

        String streamArn = dynamoDBClient.describeTable(
                new DescribeTableRequest()
                        .withTableName(tableName)).getTable().getLatestStreamArn();

        return new KinesisClientLibConfiguration(
                Constants.KCL_WORKER_APPLICATION_NAME_PREFIX + tableName,
                streamArn,
                awsCredentialsProvider,
                Constants.KCL_WORKER_APPLICATION_NAME_PREFIX + tableName + Constants.KCL_WORKER_NAME_PREFIX + taskid)

                // worker will call record processor even if there is no new data,
                // giving worker a chance to checkpoint previously queued and now committed data.
                .withCallProcessRecordsEvenForEmptyRecordList(true)

                // worker will use checkpoint tableName if available, otherwise it is safer
                // to start at beginning of the stream
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

                // we want the maximum batch size to avoid network transfer latency overhead
                .withMaxRecords(Constants.STREAMS_RECORDS_LIMIT)

                // wait a reasonable amount of time - default 0.5 seconds
                .withIdleTimeBetweenReadsInMillis(Constants.IDLE_TIME_BETWEEN_READS)

                // make parent shard poll interval tunable to decrease time to run integration test
                .withParentShardPollIntervalMillis(Constants.DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS)

                // avoid losing leases too often - default 60 seconds
                .withFailoverTimeMillis(Constants.KCL_FAILOVER_TIME)

                // logs warning if RecordProcessor task is blocked for long time periods.
                .withLogWarningForTaskAfterMillis(60 * 1000)

                // fix some random issues with https://github.com/awslabs/amazon-kinesis-client/issues/164
                .withIgnoreUnexpectedChildShards(true)

                // custom streams API endpoint
                .withDynamoDBEndpoint(endpoint)

                // custom table billing mode
                .withBillingMode(kclTableBillingMode);
    }

    /**
     * For tests
     */
    public Worker getWorker() {
        return worker;
    }
}
