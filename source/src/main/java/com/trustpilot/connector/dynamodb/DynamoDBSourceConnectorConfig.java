package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Map;

public class DynamoDBSourceConnectorConfig extends AbstractConfig {
	public static final String AWS_GROUP = "AWS";
  	public static final String CONNECTOR_GROUP = "Connector";

  	public static final String SRC_INIT_SYNC_DELAY_CONFIG = "init.sync.delay.period";
	public static final String SRC_INIT_SYNC_DELAY_DOC = "Define how long to delay INIT_SYNC start in seconds.";
	public static final String SRC_INIT_SYNC_DELAY_DISPLAY = "INIT_SYNC delay";
	public static final int SRC_INIT_SYNC_DELAY_DEFAULT = 60;

    public static final String SRC_INIT_SYNC_DISABLE_CONFIG = "init.sync.disable";
    public static final String SRC_INIT_SYNC_DISABLE_DOC = "Disabling INIT_SYNC will start from TRIM_HORIZON - 24 hours old events";
	public static final String SRC_INIT_SYNC_DISABLE_DISPLAY = "INIT_SYNC disable";
	public static final boolean SRC_INIT_SYNC_DISABLE_DEFAULT = false;

  	public static final String AWS_REGION_CONFIG = "aws.region";
	public static final String AWS_REGION_DOC = "Define AWS region.";
	public static final String AWS_REGION_DISPLAY = "Region";
	public static final String AWS_REGION_DEFAULT = "eu-west-1";

	public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
	public static final String AWS_ACCESS_KEY_ID_DOC = "Explicit AWS access key ID. Leave empty to utilize the default credential provider chain.";
	public static final String AWS_ACCESS_KEY_ID_DISPLAY = "Access key id";
	public static final Password AWS_ACCESS_KEY_ID_DEFAULT = null;

	public static final String AWS_SECRET_KEY_CONFIG = "aws.secret.key";
	public static final String AWS_SECRET_KEY_DOC = "Explicit AWS secret access key. Leave empty to utilize the default credential provider chain.";
	public static final String AWS_SECRET_KEY_DISPLAY = "Secret key";
	public static final Password AWS_SECRET_KEY_DEFAULT = null;

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

	public static final String SRC_DYNAMODB_TABLE_WHITELIST_CONFIG = "dynamodb.table.whitelist";
	public static final String SRC_DYNAMODB_TABLE_WHITELIST_DOC = "Define whitelist of dynamodb table names. This overrides table auto-discovery by ingestion tag.";
	public static final String SRC_DYNAMODB_TABLE_WHITELIST_DISPLAY = "Tables whitelist";
	public static final String SRC_DYNAMODB_TABLE_WHITELIST_DEFAULT = null;

	public static final String SRC_KCL_TABLE_BILLING_MODE_CONFIG = "kcl.table.billing.mode";
	public static final String SRC_KCL_TABLE_BILLING_MODE_DOC = "Define billing mode for internal table created by the KCL library. Default is provisioned.";
	public static final String SRC_KCL_TABLE_BILLING_MODE_DISPLAY = "KCL table billing mode";
	public static final String SRC_KCL_TABLE_BILLING_MODE_DEFAULT = "PROVISIONED";

	public static final String AWS_ASSUME_ROLE_ARN_CONFIG = "aws.assume.role.arn";
	public static final String AWS_ASSUME_ROLE_ARN_DOC = "Define which role arn the KCL/Dynamo Client should assume.";
	public static final String AWS_ASSUME_ROLE_ARN_DISPLAY = "Assume Role Arn";
	public static final String AWS_ASSUME_ROLE_ARN_DEFAULT = null;

	public static final String DST_TOPIC_PREFIX_CONFIG = "kafka.topic.prefix";
	public static final String DST_TOPIC_PREFIX_DOC = "Define Kafka topic destination prefix. End will be the name of a table.";
	public static final String DST_TOPIC_PREFIX_DISPLAY = "Topic prefix";
	public static final String DST_TOPIC_PREFIX_DEFAULT = "dynamodb-";


	public static final String REDISCOVERY_PERIOD_CONFIG = "connect.dynamodb.rediscovery.period";
	public static final String REDISCOVERY_PERIOD_DOC = "Time period in milliseconds to rediscover stream enabled DynamoDB tables";
	public static final String REDISCOVERY_PERIOD_DISPLAY = "Rediscovery period";
	public static final long REDISCOVERY_PERIOD_DEFAULT = 1 * 60 * 1000; // 1 minute

	public static final String AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG = "resource.tagging.service.endpoint";
	public static final String AWS_RESOURCE_TAGGING_API_ENDPOINT_DOC = "AWS Resource Group Tag API Endpoint. Will use default AWS if not set.";
	public static final String AWS_RESOURCE_TAGGING_API_ENDPOINT_DISPLAY = "AWS Resource Group Tag API Endpoint";
	public static final String AWS_RESOURCE_TAGGING_API_ENDPOINT_DEFAULT = null;

	public static final String AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG = "dynamodb.service.endpoint";
	public static final String AWS_DYNAMODB_SERVICE_ENDPOINT_DOC = "AWS DynamoDB API Endpoint. Will use default AWS if not set.";
	public static final String AWS_DYNAMODB_SERVICE_ENDPOINT_DISPLAY = "AWS DynamoDB API Endpoint";
	public static final String AWS_DYNAMODB_SERVICE_ENDPOINT_DEFAULT = null;

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

				.define(AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG,
						ConfigDef.Type.STRING,
						AWS_DYNAMODB_SERVICE_ENDPOINT_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_DYNAMODB_SERVICE_ENDPOINT_DOC,
						AWS_GROUP, 7,
						ConfigDef.Width.MEDIUM,
						AWS_DYNAMODB_SERVICE_ENDPOINT_DISPLAY)

				.define(AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG,
						ConfigDef.Type.STRING,
						AWS_RESOURCE_TAGGING_API_ENDPOINT_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_RESOURCE_TAGGING_API_ENDPOINT_DOC,
						AWS_GROUP, 8,
						ConfigDef.Width.MEDIUM,
						AWS_RESOURCE_TAGGING_API_ENDPOINT_DISPLAY)

				.define(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG,
						ConfigDef.Type.LIST,
						SRC_DYNAMODB_TABLE_WHITELIST_DEFAULT,
						ConfigDef.Importance.LOW,
						SRC_DYNAMODB_TABLE_WHITELIST_DOC,
						AWS_GROUP, 8,
						ConfigDef.Width.MEDIUM,
						SRC_DYNAMODB_TABLE_WHITELIST_DISPLAY)

				.define(SRC_KCL_TABLE_BILLING_MODE_CONFIG,
						ConfigDef.Type.STRING,
						SRC_KCL_TABLE_BILLING_MODE_DEFAULT,
						ConfigDef.Importance.LOW,
						SRC_KCL_TABLE_BILLING_MODE_DOC,
						AWS_GROUP, 9,
						ConfigDef.Width.MEDIUM,
						SRC_KCL_TABLE_BILLING_MODE_DISPLAY)

				.define(AWS_ASSUME_ROLE_ARN_CONFIG,
						ConfigDef.Type.STRING,
						AWS_ASSUME_ROLE_ARN_DEFAULT,
						ConfigDef.Importance.LOW,
						AWS_ASSUME_ROLE_ARN_DOC,
						AWS_GROUP, 10,
						ConfigDef.Width.LONG,
						AWS_ASSUME_ROLE_ARN_DISPLAY)

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

                .define(SRC_INIT_SYNC_DISABLE_CONFIG,
						ConfigDef.Type.BOOLEAN,
						SRC_INIT_SYNC_DISABLE_DEFAULT,
						ConfigDef.Importance.HIGH,
						SRC_INIT_SYNC_DISABLE_DOC,
						CONNECTOR_GROUP, 3,
						ConfigDef.Width.SHORT,
						SRC_INIT_SYNC_DISABLE_DISPLAY)
				;

	}

	public static void main(String[] args) {
		System.out.println(config.toRst());
	}

	public String getAwsRegion() {
		return getString(AWS_REGION_CONFIG);
	}

	public Password getAwsAccessKeyId() {
		return getPassword(AWS_ACCESS_KEY_ID_CONFIG);
	}

	public String getAwsAccessKeyIdValue() {
		return getPassword(AWS_ACCESS_KEY_ID_CONFIG)  == null ? null : getPassword(AWS_ACCESS_KEY_ID_CONFIG).value();
	}

	public Password getAwsSecretKey() {
		return getPassword(AWS_SECRET_KEY_CONFIG);
	}

	public String getAwsSecretKeyValue() {
		return getPassword(AWS_SECRET_KEY_CONFIG)  == null ? null : getPassword(AWS_SECRET_KEY_CONFIG).value();
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

    public boolean getInitSyncDisable() {
        return getBoolean(SRC_INIT_SYNC_DISABLE_CONFIG);
    }

	public String getDynamoDBServiceEndpoint() {
		return getString(AWS_DYNAMODB_SERVICE_ENDPOINT_CONFIG);
	}

	public String getResourceTaggingServiceEndpoint() {
		return getString(AWS_RESOURCE_TAGGING_API_ENDPOINT_CONFIG);
	}

	public List<String> getWhitelistTables() {
		return getList(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG) != null ? getList(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG) : null;
	}

	public BillingMode getKCLTableBillingMode() {
		return BillingMode.fromValue(getString(SRC_KCL_TABLE_BILLING_MODE_CONFIG));
	}

	public String getAwsAssumeRoleArn() {
		return getString(AWS_ASSUME_ROLE_ARN_CONFIG);
	}

}