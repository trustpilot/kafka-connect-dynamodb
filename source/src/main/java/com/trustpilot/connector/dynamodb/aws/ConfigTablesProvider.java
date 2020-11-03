package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.trustpilot.connector.dynamodb.DynamoDBSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class ConfigTablesProvider extends TablesProviderBase implements TablesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigTablesProvider.class);
    List<String> tables;
    AmazonDynamoDB client;

    public ConfigTablesProvider(AmazonDynamoDB client, DynamoDBSourceConnectorConfig connectorConfig) {
        tables = connectorConfig.getWhitelistTables();
        this.client = client;
    }

    @Override
    public List<String> getConsumableTables() {
        final List<String> consumableTables = new LinkedList<>();
        for (String table : tables) {
            try {
                final TableDescription tableDesc = client.describeTable(table).getTable();

                if (this.hasValidConfig(tableDesc, table)) {
                    LOGGER.info("Table to sync: {}", table);
                    consumableTables.add(table);
                }
            }
            catch (AmazonDynamoDBException ex) {
                LOGGER.warn("Error while trying to read metadata for table " + table, ex);
            }
        }
        return consumableTables;
    }
}
