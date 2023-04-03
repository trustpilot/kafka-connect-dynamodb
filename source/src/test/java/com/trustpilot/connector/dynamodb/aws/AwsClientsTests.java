package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AwsClientsTests {

    @Test
    public void stsAssumeRoleProviderReturned() {
        String testRoleArn = "arn:aws:iam::111111111111:role/unit-test";
        AWSCredentialsProvider provider = AwsClients.getCredentials(
                null,
                null,
                testRoleArn
        );

        DefaultAWSCredentialsProviderChain testChain = Mockito.mock(DefaultAWSCredentialsProviderChain.class);
        STSAssumeRoleSessionCredentialsProvider expectedProvider = new STSAssumeRoleSessionCredentialsProvider(
                testChain.getInstance(),
                testRoleArn,
                "kafkaconnect"
        );
        assertEquals(provider.getClass(), expectedProvider.getClass());
    }

    @Test
    public void defaultProviderReturned() {
        AWSCredentialsProvider provider = AwsClients.getCredentials(
                null,
                null,
                null
        );

        assertEquals(provider.getClass(), DefaultAWSCredentialsProviderChain.class);
    }

    @Test
    public void staticCredentialsReturned() {
        AWSCredentialsProvider provider = AwsClients.getCredentials(
                "unit-test",
                "unit-test",
                null
        );

        assertEquals(provider.getClass(), AWSStaticCredentialsProvider.class);
    }
}