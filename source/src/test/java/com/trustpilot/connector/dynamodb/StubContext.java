package com.trustpilot.connector.dynamodb;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Map;

public class StubContext implements SourceTaskContext {

    private final Map<String, String> configs;
    private final StubOffsetStorageReader offsetReader;

    public StubContext(Map<String, String> configs, StubOffsetStorageReader offsetReader) {
        this.configs = configs;
        this.offsetReader = offsetReader;
    }

    @Override
    public Map<String, String> configs() {
            return configs;
        }

    @Override
    public OffsetStorageReader offsetStorageReader() {
            return offsetReader;
        }
    }
