package com.trustpilot.connector.dynamodb.kcl;

public interface KclWorker {
    void start(String awsRegion, String tableName, String taskid);

    void stop();
}
