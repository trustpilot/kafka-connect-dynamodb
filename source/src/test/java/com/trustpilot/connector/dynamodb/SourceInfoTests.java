package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SourceInfoTests {

    @Test
    public void startInitSyncSetsState() {
        // Arrange
        SourceInfo sourceInfo = new SourceInfo("testTable1", Clock.fixed(Instant.parse("2001-01-01T00:00:00Z"),
                                                                         ZoneId.of("UTC")));

        // Act
        sourceInfo.startInitSync();

        // Assert
        assertAll("Should set correct state and start date",
                  () -> assertEquals(InitSyncStatus.RUNNING, sourceInfo.initSyncStatus),
                  () -> assertEquals(Instant.parse("2001-01-01T00:00:00Z"), sourceInfo.lastInitSyncStart),
                  () -> assertNull(sourceInfo.lastInitSyncEnd),
                  () -> assertNull(sourceInfo.exclusiveStartKey)
        );
    }

    @Test
    public void endInitSyncSetsState() {
        // Arrange
        SourceInfo sourceInfo = new SourceInfo("testTable1", Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));
        sourceInfo.lastInitSyncStart = Instant.parse("2001-01-01T00:00:00Z");

        // Act
        sourceInfo.endInitSync();

        // Assert
        assertAll("Should set correct state and end date",
                  () -> assertEquals(InitSyncStatus.FINISHED, sourceInfo.initSyncStatus),
                  () -> assertEquals(Instant.parse("2001-01-01T00:00:00Z"), sourceInfo.lastInitSyncStart),
                  () -> assertEquals(Instant.parse("2001-01-02T00:00:00Z"), sourceInfo.lastInitSyncEnd)
        );
    }

    @Test
    public void toStruct() {
        // Arrange
        SourceInfo sourceInfo = new SourceInfo("testTable1", Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));
        sourceInfo.initSyncStatus = InitSyncStatus.FINISHED;
        sourceInfo.lastInitSyncStart = Instant.parse("2001-01-01T00:00:00Z");
        sourceInfo.lastInitSyncEnd = Instant.parse("2001-01-02T00:00:00Z");

        // Act
        Struct struct = SourceInfo.toStruct(sourceInfo);

        // Assert
        struct.validate();
        assertAll("Should set correct struct values",
                  () -> assertEquals("1.0", struct.getString(SourceInfo.VERSION)),
                  () -> assertEquals("testTable1", struct.getString(SourceInfo.TABLE_NAME)),
                  () -> assertEquals("FINISHED", struct.getString(SourceInfo.INIT_SYNC_STATE)),
                  () -> assertEquals(978307200000L, struct.getInt64(SourceInfo.INIT_SYNC_START)),
                  () -> assertEquals(978393600000L, struct.getInt64(SourceInfo.INIT_SYNC_END))
        );
    }

    @Test
    public void toOffset() {
        // Arrange
        SourceInfo sourceInfo = new SourceInfo("testTable1", Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));
        sourceInfo.initSyncStatus = InitSyncStatus.FINISHED;
        sourceInfo.lastInitSyncStart = Instant.parse("2001-01-01T00:00:00Z");
        sourceInfo.lastInitSyncEnd = Instant.parse("2001-01-02T00:00:00Z");
        sourceInfo.exclusiveStartKey = Collections.singletonMap("t1", new AttributeValue("v1"));

        // Act
        Map<String, Object> offset = SourceInfo.toOffset(sourceInfo);

        // Assert
        assertAll("Should set correct map values",
                  () -> assertEquals("1.0", offset.get(SourceInfo.VERSION)),
                  () -> assertEquals("testTable1", offset.get(SourceInfo.TABLE_NAME)),
                  () -> assertEquals("FINISHED", offset.get(SourceInfo.INIT_SYNC_STATE)),
                  () -> assertEquals(978307200000L, offset.get(SourceInfo.INIT_SYNC_START)),
                  () -> assertEquals(978393600000L, offset.get(SourceInfo.INIT_SYNC_END)),
                  () -> assertEquals("{\"t1\":{\"s\":\"v1\"}}", offset.get(SourceInfo.EXCLUSIVE_START_KEY))
                  );
    }

    @Test
    public void fromOffset() {
        // Arrange
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.VERSION, "1.0");
        offset.put(SourceInfo.TABLE_NAME, "testTable11");
        offset.put(SourceInfo.INIT_SYNC_STATE, "RUNNING");
        offset.put(SourceInfo.INIT_SYNC_START, 978307200000L);
        offset.put(SourceInfo.INIT_SYNC_END, 978393600000L);
        offset.put(SourceInfo.EXCLUSIVE_START_KEY, "{\"t1\":{\"s\":\"v1\"}}");

        // Act
        SourceInfo sourceInfo = SourceInfo.fromOffset(offset, Clock.fixed(Instant.parse("2001-01-02T00:00:00Z"), ZoneId.of("UTC")));

        // Assert
        assertAll("From offset map all values",
                  () -> assertEquals("1.0", sourceInfo.version),
                  () -> assertEquals("testTable11", sourceInfo.tableName),
                  () -> assertEquals(InitSyncStatus.RUNNING, sourceInfo.initSyncStatus),
                  () -> assertEquals(Instant.parse("2001-01-01T00:00:00Z"), sourceInfo.lastInitSyncStart),
                  () -> assertEquals(Instant.parse("2001-01-02T00:00:00Z"), sourceInfo.lastInitSyncEnd),
                  () -> assertEquals(Collections.singletonMap("t1", new AttributeValue("v1")), sourceInfo.exclusiveStartKey)
                  );

    }


}


