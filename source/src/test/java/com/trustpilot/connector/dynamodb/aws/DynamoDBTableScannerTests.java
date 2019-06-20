package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.trustpilot.connector.dynamodb.SourceInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class DynamoDBTableScannerTests {

    private AmazonDynamoDB getClient(List<Map<String, AttributeValue>> returnItems,
                                     Map<String, AttributeValue> returnExclusiveStartKey) {
        AmazonDynamoDB dynamoDbClient = Mockito.mock(AmazonDynamoDB.class);

        ConsumedCapacity consumedCapacity = new ConsumedCapacity();
        consumedCapacity.setCapacityUnits((double) 15);

        ScanResult scanResult = new ScanResult();
        scanResult.setConsumedCapacity(consumedCapacity);
        scanResult.setLastEvaluatedKey(returnExclusiveStartKey);

        scanResult.setItems(returnItems);

        when(dynamoDbClient.scan(ArgumentMatchers.any()))
                .thenReturn(scanResult);

        return dynamoDbClient;
    }

    @Test
    public void scannedItemsAreReturned() {
        // Arrange
        String table = "TestTable1";
        List<Map<String, AttributeValue>> expectedItems = new LinkedList<>();
        expectedItems.add(Collections.singletonMap("Col1", new AttributeValue("val1")));

        AmazonDynamoDB client = getClient(expectedItems, null);
        DynamoDBTableScanner scanner = new DynamoDBTableScanner(client, table, 15L);
        SourceInfo sourceInfo = new SourceInfo(table, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));

        // Act
        ScanResult receivedItems = scanner.getItems(sourceInfo.exclusiveStartKey);

        // Assert
        ScanRequest expectedScanRequest = new ScanRequest()
                .withTableName(table)
                .withLimit(1000)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExclusiveStartKey(null);
        verify(client, only()).scan(ArgumentMatchers.eq(expectedScanRequest));

        assertEquals(expectedItems, receivedItems.getItems());
    }

        @Test
    public void scannedItemsAreReturnedForOnDemandCapacityTable() {
        // Arrange
        String table = "TestTable1";
        List<Map<String, AttributeValue>> expectedItems = new LinkedList<>();
        expectedItems.add(Collections.singletonMap("Col1", new AttributeValue("val1")));

        AmazonDynamoDB client = getClient(expectedItems, null);
        DynamoDBTableScanner scanner = new DynamoDBTableScanner(client, table, 0L);
        SourceInfo sourceInfo = new SourceInfo(table, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));

        // Act
        ScanResult receivedItems = scanner.getItems(sourceInfo.exclusiveStartKey);

        // Assert
        ScanRequest expectedScanRequest = new ScanRequest()
                .withTableName(table)
                .withLimit(1000)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExclusiveStartKey(null);
        verify(client, only()).scan(ArgumentMatchers.eq(expectedScanRequest));

        assertEquals(expectedItems, receivedItems.getItems());
    }

    @Test
    public void exclusiveStartKeyIsUsedFromSourceInfo() {
        // Arrange
        String table = "TestTable1";

        AmazonDynamoDB client = getClient(null, null);
        DynamoDBTableScanner scanner = new DynamoDBTableScanner(client, table, 15L);

        Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();

        SourceInfo sourceInfo = new SourceInfo(table, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));
        sourceInfo.exclusiveStartKey = exclusiveStartKey;

        // Act
        scanner.getItems(sourceInfo.exclusiveStartKey);

        // Assert
        ScanRequest expectedScanRequest = new ScanRequest()
                .withTableName(table)
                .withLimit(1000)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExclusiveStartKey(exclusiveStartKey);
        verify(client, only()).scan(ArgumentMatchers.eq(expectedScanRequest));
    }

    @Test
    public void exclusiveStartKeyIsSet() {
        String table = "TestTable1";
        Map<String, AttributeValue> exclusiveStartKey = new HashMap<>();

        AmazonDynamoDB client = getClient(null, exclusiveStartKey);
        DynamoDBTableScanner scanner = new DynamoDBTableScanner(client, table, 15L);

        SourceInfo sourceInfo = new SourceInfo(table, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));

        // Act
        ScanResult scanResult = scanner.getItems(sourceInfo.exclusiveStartKey);

        // Assert
        assertEquals(exclusiveStartKey, scanResult.getLastEvaluatedKey());
    }

}
