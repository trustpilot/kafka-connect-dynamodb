package com.trustpilot.connector.dynamodb.aws;

import java.util.List;

public class StaticTablesProvider implements TablesProvider {

    private final List<String> tables;

    public StaticTablesProvider(List<String> tables) {
        if (tables == null || tables.isEmpty()) {
            throw new RuntimeException("Misconfiguration");
        }
        this.tables = tables;
    }

    @Override
    public List<String> getConsumableTables() {
        return tables;
    }
}
