package com.trustpilot.connector.dynamodb;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DynamoDBSourceTaskConfig extends DynamoDBSourceConnectorConfig {
    public static final String TABLE_NAME_CONFIG = "table";
    private static final String TABLE_NAME_DOC = "table for this task to watch for changes.";

    public static final String TASK_ID_CONFIG = "task.id";
    private static final String TASK_ID_DOC = "Per table id of the task.";

    private static final ConfigDef config = baseConfigDef()
            .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TABLE_NAME_DOC)
            .define(TASK_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TASK_ID_DOC);

    public DynamoDBSourceTaskConfig(Map<String, String> props) {
        super(config, props);
    }

    public String getTableName() {
        return getString(TABLE_NAME_CONFIG);
    }

    public String getTaskID() {
        return getString(TASK_ID_CONFIG);
    }
}
