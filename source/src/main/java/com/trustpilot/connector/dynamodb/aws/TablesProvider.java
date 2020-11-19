package com.trustpilot.connector.dynamodb.aws;

import java.util.List;

public interface TablesProvider {
    List<String> getConsumableTables() throws InterruptedException;
}

