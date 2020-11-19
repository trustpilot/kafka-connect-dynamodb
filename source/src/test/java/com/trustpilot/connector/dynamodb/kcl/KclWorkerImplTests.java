package com.trustpilot.connector.dynamodb.kcl;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class KclWorkerImplTests {

    private ArrayBlockingQueue<KclRecordsWrapper> queue;
    private ConcurrentHashMap<String, ShardInfo> shardRegister;

    @BeforeEach
    void init() {
        queue = new ArrayBlockingQueue<>(2);
        shardRegister = new ConcurrentHashMap<>();
    }

    @Test
    void initializationRegistersNewShardToRegistry() {
        // Arrange
        KclWorkerImpl kclWorker = new KclWorkerImpl(null, queue, shardRegister);
        String tableName = "testTableName1";
        String taskId = "task1";
        String serviceEndpoint = "http://localhost:8000";
        BillingMode kclTableBillingMode = BillingMode.PROVISIONED;

        AmazonDynamoDB dynamoDBClient = Mockito.mock(AmazonDynamoDB.class);
        TableDescription table = new TableDescription().withTableArn("testArn1");
        DescribeTableResult result = new DescribeTableResult().withTable(table);
        when(dynamoDBClient.describeTable(ArgumentMatchers.<DescribeTableRequest>any())).thenReturn(result);

        // Act
        KinesisClientLibConfiguration clientLibConfiguration = kclWorker.getClientLibConfiguration(tableName, taskId, dynamoDBClient, serviceEndpoint, kclTableBillingMode);

        // Assert
        assertEquals("datalake-KCL-testTableName1", clientLibConfiguration.getApplicationName());
        assertEquals("datalake-KCL-testTableName1-worker-task1", clientLibConfiguration.getWorkerIdentifier());
        assertEquals(serviceEndpoint, clientLibConfiguration.getDynamoDBEndpoint());
    }
}
