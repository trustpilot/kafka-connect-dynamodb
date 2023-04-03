package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPI;
import com.amazonaws.services.resourcegroupstaggingapi.AWSResourceGroupsTaggingAPIClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AwsClients {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsClients.class);

    public static AmazonDynamoDB buildDynamoDbClient(String awsRegion,
                                                     String serviceEndpoint,
                                                     String awsAccessKeyID,
                                                     String awsSecretKey,
                                                     String awsAssumeRoleArn) {

        return (AmazonDynamoDB) configureBuilder(
                AmazonDynamoDBClientBuilder.standard(),
                awsRegion, serviceEndpoint,
                awsAccessKeyID,
                awsSecretKey,
                awsAssumeRoleArn)
                .build();
    }

    public static AWSResourceGroupsTaggingAPI buildAWSResourceGroupsTaggingAPIClient(String awsRegion,
                                                                                     String serviceEndpoint,
                                                                                     String awsAccessKeyID,
                                                                                     String awsSecretKey,
                                                                                     String awsAssumeRoleArn) {
        return (AWSResourceGroupsTaggingAPI) configureBuilder(
                AWSResourceGroupsTaggingAPIClientBuilder.standard(),
                awsRegion, serviceEndpoint,
                awsAccessKeyID,
                awsSecretKey,
                awsAssumeRoleArn)
                .build();
    }

    public static AmazonDynamoDBStreams buildDynamoDbStreamsClient(String awsRegion,
                                                                   String serviceEndpoint,
                                                                   String awsAccessKeyID,
                                                                   String awsSecretKey,
                                                                   String awsAssumeRoleArn) {
        return (AmazonDynamoDBStreams) configureBuilder(
                AmazonDynamoDBStreamsClientBuilder.standard(),
                awsRegion, serviceEndpoint,
                awsAccessKeyID,
                awsSecretKey,
                awsAssumeRoleArn)
                .build();

    }

    public static AWSCredentialsProvider getCredentials(String awsAccessKeyID,
                                                        String awsSecretKey,
                                                        String awsAssumeRoleArn) {
        if (awsAssumeRoleArn != null ) {
            LOGGER.debug("Using STSAssumeRoleSessionCredentialsProvider");
            AWSCredentialsProvider awsCredentialsProviderChain = DefaultAWSCredentialsProviderChain.getInstance();
            return new STSAssumeRoleSessionCredentialsProvider(awsCredentialsProviderChain,
                    awsAssumeRoleArn, "kafkaconnect");
        } else if (awsAccessKeyID == null || awsSecretKey == null) {
            LOGGER.debug("Using DefaultAWSCredentialsProviderChain");

            return DefaultAWSCredentialsProviderChain.getInstance();
        } else {
            LOGGER.debug("Using AWS credentials from connector configuration");

            final BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsAccessKeyID, awsSecretKey);
            return new AWSStaticCredentialsProvider(awsCreds);
        }
    }

    private static AwsClientBuilder configureBuilder(AwsClientBuilder builder,
                                                     String awsRegion,
                                                     String serviceEndpoint,
                                                     String awsAccessKeyID,
                                                     String awsSecretKey,
                                                     String awsAssumeRoleArn) {

        builder.withCredentials(getCredentials(awsAccessKeyID, awsSecretKey, awsAssumeRoleArn))
                .withClientConfiguration(new ClientConfiguration().withThrottledRetries(true));

        if(serviceEndpoint != null && !serviceEndpoint.isEmpty()) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, awsRegion));
        } else {
            builder.withRegion(awsRegion);
        }
        return builder;
    }
}