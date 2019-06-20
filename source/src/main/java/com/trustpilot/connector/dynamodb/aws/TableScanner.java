package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.Map;

public interface TableScanner {
    ScanResult getItems(Map<String, AttributeValue> exclusiveStartKey);
}
