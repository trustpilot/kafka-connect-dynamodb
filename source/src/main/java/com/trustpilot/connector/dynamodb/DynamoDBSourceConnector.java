package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.trustpilot.connector.dynamodb.aws.AwsClients;
import com.trustpilot.connector.dynamodb.aws.ConfigTablesProvider;
import com.trustpilot.connector.dynamodb.aws.DynamoDBTablesProvider;
import com.trustpilot.connector.dynamodb.aws.TablesProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Connector configures and starts one source task per "configure" DynamoDB table.
 * Tables are configured if they start with one of the configured prefixes and contain configured TAG.
 * <p>
 * NOTE: configured tasks count should not exceed available `maxTasks`. If it does, no tasks will be started.
 * <p>
 * This connector implementation does not support more then one Kafka Connect task per table!
 */
public class DynamoDBSourceConnector extends SourceConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBSourceConnector.class);

    private Map<String, String> configProperties;
    private TablesProvider tablesProvider;

    private List<String> consumableTables;

    private volatile Timer timer;

    @SuppressWarnings("unused")
    //Used by Confluent platform to initialize connector
    public DynamoDBSourceConnector() {
    }

    DynamoDBSourceConnector(TablesProvider tablesProvider) {
        this.tablesProvider = tablesProvider;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DynamoDBSourceTask.class;
    }


    @Override
    public void start(Map<String, String> properties) {
        configProperties = properties;
        DynamoDBSourceConnectorConfig config = new DynamoDBSourceConnectorConfig(configProperties);

        AWSResourceGroupsTaggingAPI groupsTaggingAPIClient =
                AwsClients.buildAWSResourceGroupsTaggingAPIClient(config.getAwsRegion(),
                                                                  config.getResourceTaggingServiceEndpoint(),
                                                                  config.getAwsAccessKeyIdValue(),
                                                                  config.getAwsSecretKeyValue());

        AmazonDynamoDB dynamoDBClient = AwsClients.buildDynamoDbClient(config.getAwsRegion(),
                                                                       config.getDynamoDBServiceEndpoint(),
                                                                       config.getAwsAccessKeyIdValue(),
                                                                       config.getAwsSecretKeyValue());

        if (tablesProvider == null) {
            if (config.getWhitelistTables() != null) {
                tablesProvider = new ConfigTablesProvider(dynamoDBClient, config);
            } else {
                tablesProvider = new DynamoDBTablesProvider(
                        groupsTaggingAPIClient,
                        dynamoDBClient,
                        config.getSrcDynamoDBIngestionTagKey(),
                        config.getSrcDynamoDBEnvTagKey(),
                        config.getSrcDynamoDBEnvTagValue());
            }
        }

        startBackgroundReconfigurationTasks(this.context, config.getRediscoveryPeriod());
    }

    private void startBackgroundReconfigurationTasks(ConnectorContext connectorContext, long rediscoveryPeriod) {
        timer = new Timer(true);

        // Monitors DynamoDB tables for configuration changes and initiates tasks reconfiguration if needed.
        // For e.g. if new table is configured for streaming into Kafka
        TimerTask configurationChangeDetectionTask = new TimerTask() {
            @Override
            public void run() {
                try {
                    if (consumableTables != null) {
                        LOGGER.info("Looking for changed DynamoDB tables");
                        List<String> consumableTablesRefreshed = tablesProvider.getConsumableTables();
                        if (!consumableTables.equals(consumableTablesRefreshed)) {
                            LOGGER.info("Detected changes in DynamoDB tables. Requesting tasks reconfiguration.");
                            connectorContext.requestTaskReconfiguration();
                        }
                    }
                } catch (InterruptedException ignored) {
                }
            }
        };
        timer.scheduleAtFixedRate(configurationChangeDetectionTask, rediscoveryPeriod, rediscoveryPeriod);
    }

    @Override
    public void stop() {
        timer.cancel();
    }

    @Override
    public ConfigDef config() {
        return DynamoDBSourceConnectorConfig.config;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        try {
            consumableTables = tablesProvider.getConsumableTables();
        } catch (InterruptedException e) {
            LOGGER.info("Received InterruptedException while getting consumable tables.", e);
            Thread.currentThread().interrupt();
        }

        if (consumableTables.size() > maxTasks) {
            LOGGER.error("Found {} consumable tables, but maxTasks is {}. No tasks will be configured!",
                         consumableTables.size(), maxTasks);
            return null;
        }

        List<Map<String, String>> taskConfigs = new ArrayList<>(consumableTables.size());
        for (String table : consumableTables) {
            LOGGER.info("Configuring task for table {}", table);
            Map<String, String> taskProps = new HashMap<>(configProperties);

            taskProps.put(DynamoDBSourceTaskConfig.TABLE_NAME_CONFIG, table);

            // In feature we might allow having more then one task per table for performance reasons.
            // TaskID will be needed for KCL worker identifiers and also to orchestrate init sync.
            taskProps.put(DynamoDBSourceTaskConfig.TASK_ID_CONFIG, "task-1");
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public String version() {
        return "0.1";
    }
}
