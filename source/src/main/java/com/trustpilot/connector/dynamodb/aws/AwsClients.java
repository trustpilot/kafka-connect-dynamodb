package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPIClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsClients {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsClients.class);

    public static AmazonDynamoDB buildDynamoDbClient(String awsRegion,
                                                     String awsAccessKeyID,
                                                     String awsSecretKey) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setUseThrottleRetries(true);

        return AmazonDynamoDBClientBuilder.standard()
                                          .withCredentials(getCredentials(awsAccessKeyID, awsSecretKey))
                                          .withClientConfiguration(clientConfig)
                                          .withRegion(awsRegion)
                                          .build();

    }

    public static AWSResourceGroupsTaggingAPI buildAWSResourceGroupsTaggingAPIClient(String awsRegion,
                                                                                     String awsAccessKeyID,
                                                                                     String awsSecretKey) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setUseThrottleRetries(true);

        return AWSResourceGroupsTaggingAPIClientBuilder.standard()
                                                       .withCredentials(getCredentials(awsAccessKeyID, awsSecretKey))
                                                       .withClientConfiguration(clientConfig)
                                                       .withRegion(awsRegion)
                                                       .build();
    }

    public static AWSCredentialsProvider getCredentials(String awsAccessKeyID, String awsSecretKey) {
        if (awsAccessKeyID == null || awsSecretKey == null) {
            LOGGER.debug("Using DefaultAWSCredentialsProviderChain");

            return DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            LOGGER.debug("Using AWS credentials from connector configuration");

            final BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyID, awsSecretKey);
            return new AWSStaticCredentialsProvider(awsCreds);
        }
    }
}
