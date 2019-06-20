package com.trustpilot.connector.dynamodb;

import org.apache.kafka.connect.storage.OffsetStorageReader;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collection;
import java.util.Map;

public class StubOffsetStorageReader implements OffsetStorageReader {
    private final String partitionName;
    private final Map<String, Object> offset;

    public StubOffsetStorageReader(String partitionName, Map<String, Object> offset) {
        this.partitionName = partitionName;
        this.offset = offset;
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> partition) {
        if (partition.get("table_name").equals(partitionName)) {
            return offset;
        }
        return null;
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
        throw new NotImplementedException();
    }
}
