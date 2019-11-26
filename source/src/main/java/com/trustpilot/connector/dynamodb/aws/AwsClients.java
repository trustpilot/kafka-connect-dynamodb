package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPIClientBuilder;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsClients {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsClients.class);

    public static AmazonDynamoDB buildDynamoDbClient(String endpoint,
                                                     String awsRegion,
                                                     String awsAccessKeyID,
                                                     String awsSecretKey) {
        return buildDynamoDbClient(endpoint, awsRegion, getCredentials(awsAccessKeyID, awsSecretKey));
    }

    public static AmazonDynamoDB buildDynamoDbClient(String endpoint,
                                                     String awsRegion,
                                                     AWSCredentialsProvider credentialsProvider) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setUseThrottleRetries(true);

        AmazonDynamoDBClientBuilder clientBuilder = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withClientConfiguration(clientConfig);
        if (StringUtils.isNotEmpty(endpoint)) {
            clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, awsRegion));
        } else {
            clientBuilder.withRegion(awsRegion);
        }
        return clientBuilder.build();

    }

    public static AWSResourceGroupsTaggingAPI buildAWSResourceGroupsTaggingAPIClient(String endpoint,
                                                                                     String awsRegion,
                                                                                     String awsAccessKeyID,
                                                                                     String awsSecretKey) {
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setUseThrottleRetries(true);

        AWSResourceGroupsTaggingAPIClientBuilder clientBuilder = AWSResourceGroupsTaggingAPIClientBuilder.standard()
                .withCredentials(getCredentials(awsAccessKeyID, awsSecretKey))
                .withClientConfiguration(clientConfig);

        if (StringUtils.isNotEmpty(endpoint)) {
            clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, awsRegion));
        } else {
            clientBuilder.withRegion(awsRegion);
        }
        return clientBuilder.build();
    }

    public static AmazonDynamoDBStreams buildDynamoDbStreamClient(String endpoint,
                                                                  String awsRegion,
                                                                  AWSCredentialsProvider credentialsProvider) {
        AmazonDynamoDBStreamsClientBuilder clientBuilder = AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(credentialsProvider);
        if (StringUtils.isNotEmpty(endpoint)) {
            clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, awsRegion));
        } else {
            clientBuilder.withRegion(awsRegion);
        }
        return clientBuilder.build();
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
