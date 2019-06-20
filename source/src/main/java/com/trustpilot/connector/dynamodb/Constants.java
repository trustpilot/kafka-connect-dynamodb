package com.trustpilot.connector.dynamodb;

public class Constants {
    /**
     * KCL constants
     */
    public static final int IDLE_TIME_BETWEEN_READS = 500;
    public static final int STREAMS_RECORDS_LIMIT = 1000;
    public static final int KCL_FAILOVER_TIME = 10000;
    public static final long DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = 10000L;
    public static final int KCL_RECORD_PROCESSOR_CHECKPOINTING_INTERVAL = 15;
    public static final String KCL_WORKER_NAME_PREFIX = "-worker-";
    public static final String KCL_WORKER_APPLICATION_NAME_PREFIX = "datalake-KCL-";
}
