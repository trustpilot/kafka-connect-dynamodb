package com.trustpilot.connector.dynamodb.kcl;

/**
 * Tracks which records has been committed to Kafka from this Streams shard.
 */
public class ShardInfo {
    private final String shardId;

    /**
     * Sequence number of the latest record which have been committed to Kafka from this shard.
     */
    private volatile String lastCommittedRecordSeqNo = "";

    public ShardInfo(String shardId) {
        this.shardId = shardId;
    }

    public String getShardId() {
        return shardId;
    }

    public String getLastCommittedRecordSeqNo() {
        return lastCommittedRecordSeqNo;
    }

    public void setLastCommittedRecordSeqNo(String lastCommittedRecordSeqNo) {
        this.lastCommittedRecordSeqNo = lastCommittedRecordSeqNo;
    }
}
