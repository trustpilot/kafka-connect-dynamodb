package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.trustpilot.connector.dynamodb.utils.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tracks state running source task.
 *
 * {@link DynamoDBSourceTask} uses this object to commit it's state into Kafka and
 * resumes from it after recovery.
 *
 * It's also stored together with each event value sent to Kafka topic for traceability.
 */
public class SourceInfo {
    private static final Gson gson = new Gson();
    private final Clock clock;

    public final String version;
    public final String tableName;
    public boolean initSync;
    public InitSyncStatus initSyncStatus = InitSyncStatus.UNDEFINED;
    public Instant lastInitSyncStart;
    public Instant lastInitSyncEnd = null;
    public long initSyncCount = 0L;

    /**
     * Key used by DynamoDB client to scan all table records in pages.
     * We track it to be able to resume InitSyncStatus after restarts.
     *
     * Used only during InitSync.
     */
    public Map<String, AttributeValue> exclusiveStartKey = null;

    public static final String VERSION = "version";
    public static final String TABLE_NAME = "table_name";
    public static final String INIT_SYNC = "init_sync";
    public static final String INIT_SYNC_STATE = "init_sync_state";
    public static final String INIT_SYNC_START = "init_sync_start";
    public static final String INIT_SYNC_END = "init_sync_end";
    public static final String INIT_SYNC_COUNT = "init_sync_count";
    public static final String EXCLUSIVE_START_KEY = "exclusive_start_key";


    public SourceInfo(String tableName, Clock clock) {
        this.version = "1.0";
        this.tableName = tableName;
        this.clock = clock;
    }

    public void startInitSync() {
        initSyncStatus = InitSyncStatus.RUNNING;
        lastInitSyncStart = Instant.now(clock);
        lastInitSyncEnd = null;
        exclusiveStartKey = null;
        initSyncCount = 0L;
    }

    public void endInitSync() {
        initSyncStatus = InitSyncStatus.FINISHED;
        lastInitSyncEnd = Instant.now(clock);
    }

    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
                                                             .name(SchemaNameAdjuster
                                                                           .defaultAdjuster()
                                                                           .adjust("com.trustpilot.connector.dynamodb.source"))
                                                             .field(VERSION, Schema.STRING_SCHEMA)
                                                             .field(TABLE_NAME, Schema.STRING_SCHEMA)
                                                             .field(INIT_SYNC, Schema.BOOLEAN_SCHEMA)
                                                             .field(INIT_SYNC_STATE, Schema.STRING_SCHEMA)
                                                             .field(INIT_SYNC_START, Schema.INT64_SCHEMA)
                                                             .field(INIT_SYNC_END, Schema.OPTIONAL_INT64_SCHEMA)
                                                             .field(INIT_SYNC_COUNT, Schema.OPTIONAL_INT64_SCHEMA)
                                                             .build();

    public static Schema structSchema() {
        return STRUCT_SCHEMA;
    }

    public static Struct toStruct(SourceInfo sourceInfo) {
        Struct struct = new Struct(STRUCT_SCHEMA)
                .put(VERSION, sourceInfo.version)
                .put(TABLE_NAME, sourceInfo.tableName)
                .put(INIT_SYNC, sourceInfo.initSync)
                .put(INIT_SYNC_STATE, sourceInfo.initSyncStatus.toString())
                .put(INIT_SYNC_START, sourceInfo.lastInitSyncStart.toEpochMilli());

        if (sourceInfo.lastInitSyncEnd != null) {
            struct.put(INIT_SYNC_END, sourceInfo.lastInitSyncEnd.toEpochMilli());
            struct.put(INIT_SYNC_COUNT, sourceInfo.initSyncCount);
        }

        return struct;
    }

    public static Map<String, Object> toOffset(SourceInfo sourceInfo) {
        Map<String, Object> offset = new LinkedHashMap<>();
        offset.put(VERSION, sourceInfo.version);
        offset.put(TABLE_NAME, sourceInfo.tableName);
        offset.put(INIT_SYNC_STATE, sourceInfo.initSyncStatus.toString());
        offset.put(INIT_SYNC_START, sourceInfo.lastInitSyncStart.toEpochMilli());

        if (sourceInfo.exclusiveStartKey != null) {
            offset.put(EXCLUSIVE_START_KEY, gson.toJson(sourceInfo.exclusiveStartKey));
        }

        if (sourceInfo.lastInitSyncEnd != null) {
            offset.put(INIT_SYNC_END, sourceInfo.lastInitSyncEnd.toEpochMilli());
        }

        offset.put(INIT_SYNC_COUNT, sourceInfo.initSyncCount);

        return offset;
    }

    public static SourceInfo fromOffset(Map<String, Object> offset, Clock clock) {
        SourceInfo sourceInfo = new SourceInfo((String) offset.get(TABLE_NAME), clock);
        sourceInfo.initSyncStatus = InitSyncStatus.valueOf((String) offset.get(INIT_SYNC_STATE));
        sourceInfo.lastInitSyncStart = Instant.ofEpochMilli((Long) offset.get(INIT_SYNC_START));

        if (offset.containsKey(EXCLUSIVE_START_KEY)) {

            Type empMapType = new TypeToken<Map<String, AttributeValue>>() {}.getType();
            sourceInfo.exclusiveStartKey = gson.fromJson((String)offset.get(EXCLUSIVE_START_KEY), empMapType);
        }

        if (offset.containsKey(INIT_SYNC_END)) {
            sourceInfo.lastInitSyncEnd = Instant.ofEpochMilli((Long) offset.get(INIT_SYNC_END));
        }

        if (offset.containsKey(INIT_SYNC_COUNT)) {
            sourceInfo.initSyncCount = (long) offset.get(INIT_SYNC_COUNT);
        }

        return sourceInfo;
    }


    @Override
    public String toString() {
        return "SourceInfo{" +
                //", version='" + version + '\'' +
                "  tableName='" + tableName + '\'' +
                ", initSync=" + initSync +
                ", initSyncStatus=" + initSyncStatus +
                //", lastInitSyncStart=" + lastInitSyncStart +
                //", lastInitSyncEnd=" + lastInitSyncEnd +
                ", initSyncCount=" + initSyncCount +
                ", exclusiveStartKey=" + exclusiveStartKey +
                '}';
    }
}
