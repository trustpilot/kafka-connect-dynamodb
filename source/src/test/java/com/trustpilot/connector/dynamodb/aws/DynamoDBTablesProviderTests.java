package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.amazonaws.services.resourcegroupstaggingapi.model.GetResourcesRequest;
import com.amazonaws.services.resourcegroupstaggingapi.model.GetResourcesResult;
import com.amazonaws.services.resourcegroupstaggingapi.model.ResourceTagMapping;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import com.amazonaws.services.resourcegroupstaggingapi.model.Tag;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class DynamoDBTablesProviderTests {

    private DynamoDBTablesProvider getDynamoDBTablesProvider(String tableName,
                                                             Collection<Tag> tags,
                                                             StreamViewType streamViewType) {

        AWSResourceGroupsTaggingAPI groupsTaggingAPIMock = Mockito.mock(AWSResourceGroupsTaggingAPI.class);
        AmazonDynamoDB dynamoDbClientMock = Mockito.mock(AmazonDynamoDB.class);

        ResourceTagMapping resource = new ResourceTagMapping()
                .withResourceARN("arn:aws:dynamodb:region:account-id:table/" + tableName)
                .withTags(tags);

        GetResourcesResult resourcesResult = new GetResourcesResult()
                .withResourceTagMappingList(resource);

        when(groupsTaggingAPIMock.getResources(ArgumentMatchers.any())).thenReturn(resourcesResult);

        StreamSpecification streamSpecification = new StreamSpecification()
                .withStreamEnabled(false);
        if (streamViewType != null) {
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(streamViewType);
        }
        TableDescription table1Description = new TableDescription()
                .withTableArn("arn:aws:dynamodb:region:account-id:table/" + tableName)
                .withStreamSpecification(streamSpecification);
        DescribeTableResult describeTableResult = new DescribeTableResult()
                .withTable(table1Description);

        when(dynamoDbClientMock.describeTable(tableName))
                .thenReturn(describeTableResult);

        return new DynamoDBTablesProvider(groupsTaggingAPIMock, dynamoDbClientMock,
                                          "ingest", "env", "tests");
    }

    @Test
    public void returnsTableMatchingAllRequirements() throws InterruptedException {
        // Arrange
        DynamoDBTablesProvider provider = getDynamoDBTablesProvider("tPrxTable1",
                                                                    Arrays.asList(new Tag().withKey("ingest"),
                                                                                  new Tag().withKey("env")
                                                                                           .withValue("tests")),
                                                                    StreamViewType.NEW_AND_OLD_IMAGES);

        // Act
        List<String> consumableTables = provider.getConsumableTables();

        // Assert
        assertEquals("tPrxTable1", consumableTables.get(0));
    }

    @Test
    public void skipsTablesWithoutEnabledStream() throws InterruptedException {
        // Arrange
        DynamoDBTablesProvider provider = getDynamoDBTablesProvider("tPrxTable1",
                                                                    Arrays.asList(new Tag().withKey(
                                                                            "ingest"),
                                                                                  new Tag().withKey("env")
                                                                                           .withValue("tests")),
                                                                    null);

        // Act
        List<String> consumableTables = provider.getConsumableTables();

        // Assert
        assertTrue(consumableTables.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"OLD_IMAGE", "KEYS_ONLY"})
    public void skipsTablesWithWrongStreamType(String streamViewType) throws InterruptedException {
        // Arrange
        DynamoDBTablesProvider provider = getDynamoDBTablesProvider("tPrxTable1",
                                                                    Arrays.asList(new Tag().withKey("ingest"),
                                                                                  new Tag().withKey("env")
                                                                                           .withValue("tests")),
                                                                    StreamViewType.fromValue(streamViewType));

        // Act
        List<String> consumableTables = provider.getConsumableTables();

        // Assert
        assertTrue(consumableTables.isEmpty());
    }


    @Test
    public void returnsTablesUsingPaging() throws InterruptedException {
        // Arrange
        AWSResourceGroupsTaggingAPI groupsTaggingAPIMock = Mockito.mock(AWSResourceGroupsTaggingAPI.class);
        AmazonDynamoDB dynamoDbClientMock = Mockito.mock(AmazonDynamoDB.class);

        ResourceTagMapping resource1 = new ResourceTagMapping()
                .withResourceARN("arn:aws:dynamodb:region:account-id:table/table1")
                .withTags(Arrays.asList(new Tag().withKey("ingest"),
                                        new Tag().withKey("env")
                                                 .withValue("tests")));
        GetResourcesResult resourcesResult1 = new GetResourcesResult()
                .withResourceTagMappingList(resource1)
                .withPaginationToken("testPaginationToken");

        ResourceTagMapping resource2 = new ResourceTagMapping()
                .withResourceARN("arn:aws:dynamodb:region:account-id:table/table2")
                .withTags(Arrays.asList(new Tag().withKey("ingest"),
                                        new Tag().withKey("env")
                                                 .withValue("tests")));
        GetResourcesResult resourcesResult2 = new GetResourcesResult()
                .withResourceTagMappingList(resource2);

        when(groupsTaggingAPIMock.getResources(ArgumentMatchers.argThat(isPaginationToken("testPaginationToken")))).thenReturn(
                resourcesResult2);
        when(groupsTaggingAPIMock.getResources(ArgumentMatchers.argThat(isPaginationToken(null)))).thenReturn(
                resourcesResult1);


        StreamSpecification streamSpecification = new StreamSpecification()
                .withStreamEnabled(false);
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        TableDescription table1Description = new TableDescription()
                .withTableArn("arn/table1")
                .withStreamSpecification(streamSpecification);
        DescribeTableResult describeTableResult1 = new DescribeTableResult()
                .withTable(table1Description);
        when(dynamoDbClientMock.describeTable("table1"))
                .thenReturn(describeTableResult1);

        StreamSpecification streamSpecification2 = new StreamSpecification()
                .withStreamEnabled(false);
        streamSpecification2.setStreamEnabled(true);
        streamSpecification2.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        TableDescription table2Description = new TableDescription()
                .withTableArn("arn/table2")
                .withStreamSpecification(streamSpecification);
        DescribeTableResult describeTableResult2 = new DescribeTableResult()
                .withTable(table2Description);
        when(dynamoDbClientMock.describeTable("table2"))
                .thenReturn(describeTableResult2);

        DynamoDBTablesProvider provider = new DynamoDBTablesProvider(groupsTaggingAPIMock, dynamoDbClientMock,
                                                                     "ingest", "env", "tests");

        // Act
        List<String> consumableTables = provider.getConsumableTables();

        // Assert
        assertEquals("table1", consumableTables.get(0));
        assertEquals("table2", consumableTables.get(1));
    }

    public ArgumentMatcher<GetResourcesRequest> isPaginationToken(String paginationToken) {
        return argument -> {
            if (argument == null) {
                return false;
            }
            if (argument.getPaginationToken() != null) {
                return argument.getPaginationToken().equals(paginationToken);
            }
            return paginationToken == null;
        };
    }
}
