package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.amazonaws.services.resourcegroupstaggingapi.model.GetResourcesRequest;
import com.amazonaws.services.resourcegroupstaggingapi.model.GetResourcesResult;
import com.amazonaws.services.resourcegroupstaggingapi.model.ResourceTagMapping;
import com.amazonaws.services.resourcegroupstaggingapi.model.TagFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Iterates over all DynamoDB tables and returns those which match configured table name prefix plus has
 * special tag defined.
 *
 * AWS access role need to be able to get DynamoDB table description.
 *
 * Also validates if tables have streams enabled with valid options.
 * All invalid tables are skipped.
 */
public class DynamoDBTablesProvider extends TablesProviderBase implements TablesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamoDBTablesProvider.class);
    private final AWSResourceGroupsTaggingAPI groupsTaggingAPI;
    private final AmazonDynamoDB client;
    private final String ingestionTagKey;
    private final String envTagKey;
    private final String envTagValue;

    public DynamoDBTablesProvider(AWSResourceGroupsTaggingAPI groupsTaggingAPI,
                                  AmazonDynamoDB client,
                                  String ingestionTagKey,
                                  String envTagKey,
                                  String envTagValue)
    {
        this.groupsTaggingAPI = groupsTaggingAPI;
        this.client = client;
        this.ingestionTagKey = ingestionTagKey;
        this.envTagKey = envTagKey;
        this.envTagValue = envTagValue;
    }

    public List<String> getConsumableTables() {
        LOGGER.info("Searching for tables with tag.key: {}", ingestionTagKey);

        final List<String> consumableTables = new LinkedList<>();
        GetResourcesRequest resourcesRequest = getGetResourcesRequest();
        while (true) {
            GetResourcesResult result = groupsTaggingAPI.getResources(resourcesRequest);

            for (ResourceTagMapping resource : result.getResourceTagMappingList()) {
                String tableARN = resource.getResourceARN();
                String tableName = tableARN.substring(tableARN.lastIndexOf("/") + 1);

                try {
                    final TableDescription tableDesc = client.describeTable(tableName).getTable();

                    if (hasValidConfig(tableDesc, tableName)) {
                        LOGGER.info("Table to sync: {}", tableName);
                        consumableTables.add(tableName);
                    }
                }
                catch (AmazonDynamoDBException ex) {
                    LOGGER.warn("Error while trying to read metadata for table " + tableName, ex);
                }
            }

            if ((result.getPaginationToken() == null) || result.getPaginationToken().isEmpty()) {
                break;
            }
            resourcesRequest.withPaginationToken(result.getPaginationToken());
        }

        return consumableTables;
    }

    private GetResourcesRequest getGetResourcesRequest() {
        TagFilter stackTagFilter = new TagFilter();
        stackTagFilter.setKey(envTagKey);
        stackTagFilter.setValues(Collections.singletonList(envTagValue));

        TagFilter ingestionTagFilter = new TagFilter();
        ingestionTagFilter.setKey(ingestionTagKey);

        List<TagFilter> tagFilters = new LinkedList<>();
        tagFilters.add(stackTagFilter);
        tagFilters.add(ingestionTagFilter);

        GetResourcesRequest resourcesRequest = new GetResourcesRequest();
        resourcesRequest
                .withResourceTypeFilters("dynamodb")
                .withResourcesPerPage(50)
                .withTagFilters(tagFilters);
        return resourcesRequest;
    }

}
