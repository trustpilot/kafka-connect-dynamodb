package com.trustpilot.connector.dynamodb.aws;

import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TablesProviderBase implements TablesProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(TablesProviderBase.class);

    protected boolean hasValidConfig(TableDescription tableDesc, String tableName) {
        final StreamSpecification streamSpec = tableDesc.getStreamSpecification();
        if (streamSpec == null || !streamSpec.isStreamEnabled()) {
            LOGGER.warn("DynamoDB table `{}` does not have streams enabled", tableName);
            return false;
        }

        final String streamViewType = streamSpec.getStreamViewType();
        if (!streamViewType.equals(StreamViewType.NEW_IMAGE.name())
                && !streamViewType.equals(StreamViewType.NEW_AND_OLD_IMAGES.name())) {
            LOGGER.warn("DynamoDB stream view type for table `{}` is {}", tableName, streamViewType);
            return false;
        }

        return true;
    }
}

