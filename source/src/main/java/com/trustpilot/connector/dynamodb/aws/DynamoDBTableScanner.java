package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Scans DynamoDB table in batches while adhering capacity limits.
 */
@SuppressWarnings("UnstableApiUsage")
public class DynamoDBTableScanner implements TableScanner {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTableScanner.class);
    private final AmazonDynamoDB client;

    private final String tableName;
    private final RateLimiter rateLimiter;
    private int permitsToConsume = 1;

    public DynamoDBTableScanner(AmazonDynamoDB client, String tableName, long readCapacityUnits) {
        this.client = client;
        this.tableName = tableName;

        if (readCapacityUnits > 0L) {
            this.rateLimiter = RateLimiter.create(readCapacityUnits / (double)2);
        } else {
            // Table uses on demand capacity
            this.rateLimiter = null;
        }
    }

    /**
     * Scans DynamoDB table in batches. Starting from exclusiveStartKey.
     *
     * @return ScanResult contains items and exclusiveStartKey. If exclusiveStartKey is null there are no more items left.
     */
    @Override
    public ScanResult getItems(Map<String, AttributeValue> exclusiveStartKey) {
        if (rateLimiter != null) {
            // Let the rate limiter wait until our desired throughput "recharges"
            rateLimiter.acquire(permitsToConsume);
        }

        // Do the scan
        ScanRequest scan = new ScanRequest()
                .withTableName(tableName)
                .withLimit(1000)
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExclusiveStartKey(exclusiveStartKey);

        ScanResult result = client.scan(scan);

        if (rateLimiter != null) {
            // Account for the rest of the throughput we consumed,
            // now that we know how much that scan request cost
            double consumedCapacity = result.getConsumedCapacity().getCapacityUnits();
            permitsToConsume = (int) (consumedCapacity - 1.0);
            if (permitsToConsume <= 0) {
                permitsToConsume = 1;
            }
            LOGGER.debug("Left permitsToConsume: {}, {}", permitsToConsume, rateLimiter.toString());
        }

        return result;
    }
}
