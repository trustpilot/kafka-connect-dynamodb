package com.trustpilot.connector.dynamodb;

import com.trustpilot.connector.dynamodb.aws.TablesProvider;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class DynamoDBSourceConnectorTests {

    @Test
    public void taskIsConfiguredForEachConsumableTable() throws InterruptedException {
        // Arrange
        TablesProvider tablesProvider = Mockito.mock(TablesProvider.class);

        List<String> consumableTables =  Arrays.asList("t1", "t2");
        when(tablesProvider.getConsumableTables())
                .thenReturn(consumableTables);

        DynamoDBSourceConnector connector = new DynamoDBSourceConnector(tablesProvider);

        Map<String, String> properties = new HashMap<>();
        connector.start(properties);

        // Act
        List<Map<String, String>> taskConfigs =  connector.taskConfigs(2);
        connector.stop();

        // Arrange
        assertEquals(2, taskConfigs.size());
        assertEquals("t1", taskConfigs.get(0).get("table"));
        assertEquals("t2", taskConfigs.get(1).get("table"));
    }

    @Test
    public void ifConsumableTablesCountExceedMaxTasksCountNoTasksAreConfigured() throws InterruptedException {
        // Arrange
        TablesProvider tablesProvider = Mockito.mock(TablesProvider.class);

        List<String> consumableTables =  Arrays.asList("t1", "t2", "t3");
        when(tablesProvider.getConsumableTables()).thenReturn(consumableTables);

        DynamoDBSourceConnector connector = new DynamoDBSourceConnector(tablesProvider);

        Map<String, String> properties = new HashMap<>();
        connector.start(properties);

        // Act
        List<Map<String, String>> taskConfigs =  connector.taskConfigs(2);
        connector.stop();

        // Arrange
        assertNull(taskConfigs);
    }

    @Test
    public void tasksAreReconfiguredIfRediscoveryFindsChanges() throws InterruptedException {
        // Arrange
        TablesProvider tablesProvider = Mockito.mock(TablesProvider.class);
        List<String> consumableTables =  Arrays.asList("t1", "t2", "t3");
        when(tablesProvider.getConsumableTables()).thenReturn(consumableTables);

        DynamoDBSourceConnector connector = new DynamoDBSourceConnector(tablesProvider);

        Map<String, String> properties = new HashMap<>();
        properties.put("connect.task.reconfiguration.period", "100000"); // should not happen
        properties.put("connect.dynamodb.rediscovery.period", "100");

        ConnectorContextTestIml ctx = new ConnectorContextTestIml();
        connector.initialize(ctx);

        // Act
        connector.taskConfigs(2);
        consumableTables =  Arrays.asList("t1", "t2");
        when(tablesProvider.getConsumableTables()).thenReturn(consumableTables);

        connector.start(properties);
        Thread.sleep(500);
        connector.stop();

        // Arrange
        assertTrue(ctx.requestTaskReconfigurationCount.get() >= 1);
    }

    @Test
    public void tasksAreNotReconfiguredIfRediscoveryFindsNoChanges() throws InterruptedException {
        // Arrange
        TablesProvider tablesProvider = Mockito.mock(TablesProvider.class);
        List<String> consumableTables =  Arrays.asList("t1", "t2", "t3");
        when(tablesProvider.getConsumableTables()).thenReturn(consumableTables);

        DynamoDBSourceConnector connector = new DynamoDBSourceConnector(tablesProvider);

        Map<String, String> properties = new HashMap<>();
        properties.put("connect.task.reconfiguration.period", "100000"); // should not happen
        properties.put("connect.dynamodb.rediscovery.period", "100");

        ConnectorContextTestIml ctx = new ConnectorContextTestIml();
        connector.initialize(ctx);

        // Act
        connector.taskConfigs(2);
        connector.start(properties);
        Thread.sleep(500);
        connector.stop();

        // Arrange
        assertTrue(ctx.requestTaskReconfigurationCount.get() == 0);
    }

}

class ConnectorContextTestIml implements ConnectorContext {

    public final AtomicInteger requestTaskReconfigurationCount = new AtomicInteger(0);

    @Override
    public void requestTaskReconfiguration() {
        requestTaskReconfigurationCount.incrementAndGet();
    }

    @Override
    public void raiseError(Exception e) {

    }
}
