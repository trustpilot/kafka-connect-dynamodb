package com.trustpilot.connector.dynamodb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class DynamoDBSourceConnectorConfig extends AbstractConfig {
	public static final String AWS_GROUP = "AWS";
  	public static final String CONNECTOR_GROUP = "Connector";

  	public static final String SRC_INIT_SYNC_DELAY_CONFIG = "init.sync.delay.period";
	public static final String SRC_INIT_SYNC_DELAY_DOC = "Define how long to delay INIT_SYNC start in seconds.";
	public static final String SRC_INIT_SYNC_DELAY_DISPLAY = "INIT_SYNC delay";
	public static final int SRC_INIT_SYNC_DELAY_DEFAULT = 60;

  	public static final String AWS_REGION_CONFIG = "aws.region";
	public static final String AWS_REGION_DOC = "Define AWS region.";
	public static final String AWS_REGION_DISPLAY = "Region";
	public static final String AWS_REGION_DEFAULT = "eu-west-1";

	public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
	public static final String AWS_ACCESS_KEY_ID_DOC = "Explicit AWS access key ID. Leave empty to utilize the default credential provider chain.";
	public static final String AWS_ACCESS_KEY_ID_DISPLAY = "Access key id";
	public static final Object AWS_ACCESS_KEY_ID_DEFAULT = null;

	public static final String AWS_SECRET_KEY_CONFIG = "aws.secret.key";
	public static final String AWS_SECRET_KEY_DOC = "Explicit AWS secret access key. Leave empty to utilize the default credential provider chain.";
	public static final String AWS_SECRET_KEY_DISPLAY = "Secret key";
	public static final Object AWS_SECRET_KEY_DEFAULT = null;

	public static final String SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG = "dynamodb.table.ingestion.tag.key";
	public static final String SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DOC = "Define DynamoDB table tag name. Only tables with this tag key will be ingested.";
	public static final String SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DISPLAY = "Ingestion tag key name";
	public static final String SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DEFAULT = "datalake-ingest";

	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG = "dynamodb.table.env.tag.key";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DOC = "Define DynamoDB tables environment tag name. Only tables with dynamodb.table.env.tag.value value in this key will be ingested.";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DISPLAY = "Environment tag key";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DEFAULT = "environment";

	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG = "dynamodb.table.env.tag.value";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DOC = "Define environment name which must be present in dynamodb.table.env.tag.key.";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DISPLAY = "Environment";
	public static final String SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DEFAULT = "dev";

	public static final String DST_TOPIC_PREFIX_CONFIG = "kafka.topic.prefix";
	public static final String DST_TOPIC_PREFIX_DOC = "Define Kafka topic destination prefix. End will be the name of a table.";
	public static final String DST_TOPIC_PREFIX_DISPLAY = "Topic prefix";
	public static final String DST_TOPIC_PREFIX_DEFAULT = "dynamodb-";


	public static final String REDISCOVERY_PERIOD_CONFIG = "connect.dynamodb.rediscovery.period";
	public static final String REDISCOVERY_PERIOD_DOC = "Time period in milliseconds to rediscover stream enabled DynamoDB tables";
	public static final String REDISCOVERY_PERIOD_DISPLAY = "Rediscovery period";
	public static final long REDISCOVERY_PERIOD_DEFAULT = 1 * 60 * 1000; // 1 minute

	static final ConfigDef config = baseConfigDef();

	public DynamoDBSourceConnectorConfig(Map<String, String> props) {
		this(config, props);
	}

	protected DynamoDBSourceConnectorConfig(ConfigDef config, Map<String, String> props) {
		super(config, props);
	}

	public static ConfigDef baseConfigDef() {

		return new ConfigDef()
				.define(AWS_REGION_CONFIG,
						ConfigDef.Type.STRING,
						AWS_REGION_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_REGION_DOC,
						AWS_GROUP, 1,
						ConfigDef.Width.SHORT,
						AWS_REGION_DISPLAY)

				.define(AWS_ACCESS_KEY_ID_CONFIG,
						ConfigDef.Type.PASSWORD,
						AWS_ACCESS_KEY_ID_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_ACCESS_KEY_ID_DOC,
						AWS_GROUP, 2,
						ConfigDef.Width.LONG,
						AWS_ACCESS_KEY_ID_DISPLAY)

				.define(AWS_SECRET_KEY_CONFIG,
						ConfigDef.Type.PASSWORD,
						AWS_SECRET_KEY_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_SECRET_KEY_DOC,
						AWS_GROUP, 3,
						ConfigDef.Width.LONG,
						AWS_SECRET_KEY_DISPLAY)

				.define(SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG,
						ConfigDef.Type.STRING,
						SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DEFAULT,
						ConfigDef.Importance.HIGH,
						SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DOC,
						AWS_GROUP, 4,
						ConfigDef.Width.MEDIUM,
						SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_DISPLAY)

				.define(SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG,
						ConfigDef.Type.STRING,
						SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DEFAULT,
						ConfigDef.Importance.HIGH,
						SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DOC,
						AWS_GROUP, 5,
						ConfigDef.Width.MEDIUM,
						SRC_DYNAMODB_TABLE_ENV_TAG_KEY_DISPLAY)

				.define(SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG,
						ConfigDef.Type.STRING,
						SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DEFAULT,
						ConfigDef.Importance.HIGH,
						SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DOC,
						AWS_GROUP, 6,
						ConfigDef.Width.MEDIUM,
						SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_DISPLAY)

				.define(DST_TOPIC_PREFIX_CONFIG,
						ConfigDef.Type.STRING,
						DST_TOPIC_PREFIX_DEFAULT,
						ConfigDef.Importance.HIGH,
						DST_TOPIC_PREFIX_DOC,
						CONNECTOR_GROUP, 1,
						ConfigDef.Width.MEDIUM,
						DST_TOPIC_PREFIX_DISPLAY)

				.define(SRC_INIT_SYNC_DELAY_CONFIG,
						ConfigDef.Type.INT,
						SRC_INIT_SYNC_DELAY_DEFAULT,
						ConfigDef.Importance.LOW,
						SRC_INIT_SYNC_DELAY_DOC,
						CONNECTOR_GROUP, 2,
						ConfigDef.Width.MEDIUM,
						SRC_INIT_SYNC_DELAY_DISPLAY)

				.define(REDISCOVERY_PERIOD_CONFIG,
						ConfigDef.Type.LONG,
						REDISCOVERY_PERIOD_DEFAULT,
						ConfigDef.Importance.LOW,
						REDISCOVERY_PERIOD_DOC,
						CONNECTOR_GROUP, 4,
						ConfigDef.Width.MEDIUM,
						REDISCOVERY_PERIOD_DISPLAY)

				;
	}

	public static void main(String[] args) {
		System.out.println(config.toRst());
	}

	public String getAwsRegion() {
		return getString(AWS_REGION_CONFIG);
	}

	public String getAwsAccessKeyId() {
		return getString(AWS_ACCESS_KEY_ID_CONFIG);
	}

	public String getAwsSecretKey() {
		return getString(AWS_SECRET_KEY_CONFIG);
	}

	public String getSrcDynamoDBIngestionTagKey() {
		return getString(SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG);
	}

	public String getSrcDynamoDBEnvTagKey() {
		return getString(SRC_DYNAMODB_TABLE_ENV_TAG_KEY_CONFIG);
	}

	public String getSrcDynamoDBEnvTagValue() {	return getString(SRC_DYNAMODB_TABLE_ENV_TAG_VALUE_CONFIG);	}

	public String getDestinationTopicPrefix() {
		return getString(DST_TOPIC_PREFIX_CONFIG);
	}

	public long getRediscoveryPeriod() {
		return getLong(REDISCOVERY_PERIOD_CONFIG);
	}

	public int getInitSyncDelay() {
		return (int)get(SRC_INIT_SYNC_DELAY_CONFIG);
	}
}
