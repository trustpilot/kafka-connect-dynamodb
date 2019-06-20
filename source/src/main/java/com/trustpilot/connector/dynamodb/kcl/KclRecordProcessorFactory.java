package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

import java.time.Clock;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class KclRecordProcessorFactory implements IRecordProcessorFactory {
    private final String tableName;
    private final ArrayBlockingQueue<KclRecordsWrapper> eventsQueue;
    private final ConcurrentHashMap<String, ShardInfo> shardRegister;

    public KclRecordProcessorFactory(String tableName,
                                     ArrayBlockingQueue<KclRecordsWrapper> eventsQueue,
                                     ConcurrentHashMap<String, ShardInfo> shardRegister) {
        this.tableName = tableName;
        this.eventsQueue = eventsQueue;
        this.shardRegister = shardRegister;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KclRecordProcessor(tableName, eventsQueue, shardRegister, Clock.systemUTC());
    }
}
