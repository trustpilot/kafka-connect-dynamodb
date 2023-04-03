package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus;
import com.google.gson.Gson;
import com.trustpilot.connector.dynamodb.aws.AwsClients;
import com.trustpilot.connector.dynamodb.testcontainers.ConnectContainer;
import com.trustpilot.connector.dynamodb.testcontainers.DynamoDBContainer;
import com.trustpilot.connector.dynamodb.testcontainers.MockServerContainerExt;
import com.trustpilot.connector.dynamodb.testcontainers.SchemaRegistryContainer;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.JsonBody;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.restassured.RestAssured.given;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class KafkaConnectITBase {
    protected static final String AMAZON_DYNAMO_IMAGE = "amazon/dynamodb-local:1.13.5";
    protected static final String KAFKA_IMAGE = "confluentinc/cp-kafka:5.3.1";
    protected static final String SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:5.3.1";
    protected static final String CONNECT_IMAGE = "confluentinc/cp-kafka-connect:5.3.1";
    protected static final String MOCK_SERVER_IMAGE = "jamesdbloom/mockserver:mockserver-5.11.0";

    protected static final String AWS_REGION_CONFIG = "eu-west-3";
    protected static final String AWS_ACCESS_KEY_ID_CONFIG = "ABCD";
    protected static final String AWS_SECRET_KEY_CONFIG = "1234";
    protected static final String AWS_ASSUME_ROLE_ARN_CONFIG = null;
    protected static final String SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG = "datalake-ingest";

    private static Network network;
    private static KafkaContainer kafka;
    private static SchemaRegistryContainer schemaRegistry;
    private static ConnectContainer connect;
    private static DynamoDBContainer dynamodb;
    private static MockServerContainerExt mockServer;
    private static MockServerClient fakeTaggingAPI;

    @BeforeAll
    public static void dockerSetup() {
        network = Network.newNetwork();

        dynamodb = new DynamoDBContainer(DockerImageName.parse(AMAZON_DYNAMO_IMAGE))
                .withNetwork(network);

        kafka = new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE))
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:9092")
                .withNetworkAliases("broker")
                .withEmbeddedZookeeper()
                .withNetwork(network);

        schemaRegistry = new SchemaRegistryContainer(DockerImageName.parse(SCHEMA_REGISTRY_IMAGE))
                .withNetwork(network)
                .withNetworkAliases("mock-server")
                .withKafka(kafka)
                .dependsOn(kafka);

        mockServer = (MockServerContainerExt) new MockServerContainerExt(DockerImageName.parse(MOCK_SERVER_IMAGE))
                .withNetwork(network)
                .withEnv("MOCKSERVER_LIVENESS_HTTP_GET_PATH", "/health")
                .waitingFor(Wait.forHttp("/health").forStatusCode(200));

        connect = new ConnectContainer(DockerImageName.parse(CONNECT_IMAGE), kafka, schemaRegistry)
                .withNetworkAliases("connect")
                .withNetwork(network)
                .withPlugins("../build/libs/")
                .dependsOn(schemaRegistry, mockServer);

        Startables.deepStart(Stream.of(
                dynamodb,
                kafka,
                schemaRegistry,
                mockServer,
                connect
        )).join();

        fakeTaggingAPI = new MockServerClient(mockServer.getHost(), mockServer.getServerPort());
    }

    protected void mockTaggingAPIResponse(String url, String responseBody) {
        fakeTaggingAPI
                .when(request()
                        .withPath(url))
                .respond(response()
                        .withBody(new JsonBody(responseBody)));
    }

    protected void registerConnector(String name) {
        ConnectorConfig connector = createBaseConfig(name);
        connector.config.put("resource.tagging.service.endpoint", mockServer.getInternalEndpoint());
        postConnector(connector);
    }

    protected void registerConnector(String name, List<String> tablesWhitelist) {
        ConnectorConfig connector = createBaseConfig(name);
        connector.config.put("dynamodb.table.whitelist", String.join(",", tablesWhitelist));
        postConnector(connector);
    }

    private void postConnector(ConnectorConfig connector) {
        given()
                .log().all()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(new Gson().toJson(connector))
                .when()
                .post(connect.getEndpoint() + "/connectors")
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_CREATED);
    }

    protected KafkaConsumer<String, String> getConsumer() {
        return new KafkaConsumer<>(
                new HashMap<String, Object>() {{
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
                    put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                }},
                new StringDeserializer(), new StringDeserializer());
    }

    protected List<ConsumerRecord<String, String>> drain(
            KafkaConsumer<String, String> consumer,
            int expectedRecordCount) {

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();

        Unreliables.retryUntilTrue(200, TimeUnit.SECONDS, () -> {
            consumer.poll(Duration.ofMillis(2000))
                    .iterator()
                    .forEachRemaining(allRecords::add);

            return allRecords.size() == expectedRecordCount;
        });

        return allRecords;
    }

    protected CreateTableRequest newReplicationTableRequest() {
        return new CreateTableRequest()
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(5L))
                .withStreamSpecification(
                        new StreamSpecification()
                                .withStreamEnabled(true)
                                .withStreamViewType(StreamViewType.NEW_IMAGE));
    }

    protected CreateTableResult newDynamoDBTable(CreateTableRequest createTableRequest) {
        AmazonDynamoDB client = getDynamoDBClient();

        return client.createTable(createTableRequest);
    }

    protected void putDynamoDBItems(String tableName, List<Item> itemStream) {
        Table table = new DynamoDB(getDynamoDBClient()).getTable(tableName);
        itemStream.forEach(item -> table.putItem(item));
    }

    private AmazonDynamoDB getDynamoDBClient() {
        return AwsClients.buildDynamoDbClient(
                AWS_REGION_CONFIG,
                dynamodb.getEndpoint(),
                AWS_ACCESS_KEY_ID_CONFIG,
                AWS_SECRET_KEY_CONFIG,
                AWS_ASSUME_ROLE_ARN_CONFIG
        );
    }

    private ConnectorConfig createBaseConfig(String name) {
        return new ConnectorConfig(name, new HashMap<String, String>() {{
            put("name", name);
            put("connector.class", "com.trustpilot.connector.dynamodb.DynamoDBSourceConnector");
            put("aws.access.key.id", AWS_ACCESS_KEY_ID_CONFIG);
            put("aws.secret.key", AWS_SECRET_KEY_CONFIG);
            put("aws.region", AWS_REGION_CONFIG);
            put("dynamodb.service.endpoint", dynamodb.getInternalEndpoint());
            put("init.sync.delay.period", "10");
        }});
    }

    static class ConnectorConfig implements Serializable {
        private String name;
        private Map<String, String> config;

        public ConnectorConfig(String name, Map<String, String> config) {
            this.name = name;
            this.config = config;
        }
    }

    static class ResourceTags implements Serializable {
        static class Tag implements Serializable {
            public Tag(String key, String value) {
                Value = value;
                Key = key;
            }

            public String Value;
            public String Key;
        }

        static class Mapping implements Serializable {
            public String ResourceARN;
            public List<Tag> Tags;

            public Mapping(String resourceARN, List<Tag> tags) {
                ResourceARN = resourceARN;
                Tags =  tags;
            }
        }

        public List<Mapping> ResourceTagMappingList;

        public ResourceTags(List<String> tableArns, String tagName) {
            ResourceTagMappingList = tableArns
                    .stream()
                    .map(ta -> new Mapping(ta, Arrays.asList(new Tag(tagName, null))))
                    .collect(Collectors.toList());
        }

        public String asJson() {
            return new Gson().toJson(this);
        }
    }
}