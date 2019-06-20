package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

/**
 * Wrapper to pass records between {@link KclWorkerImpl} and {@link com.trustpilot.connector.dynamodb.DynamoDBSourceTask}
 * in a buffered queue.
 */
public class KclRecordsWrapper {
    private String shardId;
    private List<Record> records;

    public KclRecordsWrapper(String shardId, List<Record> records) {
        this.shardId = shardId;
        this.records = records;
    }

    public String getShardId() {
        return shardId;
    }

    public List<Record> getRecords() {
        return records;
    }
}
