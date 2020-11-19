package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.BillingMode;

public interface KclWorker {
    void start(AmazonDynamoDB dynamoDBClient,
               AmazonDynamoDBStreams dynamoDBStreamsClient,
               String tableName,
               String taskid,
               String endpoint,
               BillingMode kclTablebillingMode);

    void stop();
}