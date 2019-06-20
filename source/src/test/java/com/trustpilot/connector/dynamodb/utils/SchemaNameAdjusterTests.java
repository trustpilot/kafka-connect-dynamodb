package com.trustpilot.connector.dynamodb.utils;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static com.trustpilot.connector.dynamodb.utils.SchemaNameAdjuster.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Randall Hauch
 *
 */
@SuppressWarnings("SameParameterValue")
public class SchemaNameAdjusterTests {

    @Test
    public void shouldDetermineValidFirstCharacters() {
        String validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertTrue(isValidFullnameFirstCharacter(validChars.charAt(i)));
        }
    }

    @Test
    public void shouldDetermineValidNonFirstCharacters() {
        String validChars = ".abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
        for (int i = 0; i != validChars.length(); ++i) {
            assertTrue(isValidFullnameNonFirstCharacter(validChars.charAt(i)));
        }
    }

    @Test
    public void shouldConsiderValidFullnames() {
        assertValidFullName("test_server.connector_test.products.Key");
        assertValidFullName("t1234.connector_test.products.Key");
    }

    @Test
    public void shouldConsiderInvalidFullnames() {
        assertNotValidFullName("test-server.connector_test.products.Key");
    }

    @Test
    public void shouldReportReplacementEveryTime() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null){
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = create(handler);
        for (int i = 0; i != 20; ++i) {
            adjuster.adjust("some-invalid-fullname$");
        }
        assertEquals(20, counter.get());
        assertEquals(0, conflicts.get());
    }

    @Test
    public void shouldReportReplacementOnlyOnce() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null){
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = create(handler.firstTimeOnly());
        for (int i = 0; i != 20; ++i) {
            adjuster.adjust("some-invalid-fullname$");
        }
        assertEquals(1, counter.get());
        assertEquals(0, conflicts.get());
    }

    @Test
    public void shouldReportConflictReplacement() {
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        ReplacementOccurred handler = (original, replacement, conflict) -> {
            if (conflict != null){
                conflicts.incrementAndGet();
            }
            counter.incrementAndGet();
        };
        SchemaNameAdjuster adjuster = create(handler.firstTimeOnly());
        adjuster.adjust("some-invalid-fullname$");
        adjuster.adjust("some-invalid%fullname_");
        assertEquals(2, counter.get());
        assertEquals(1, conflicts.get());
    }

    protected void assertValidFullName(String fullName) {
        assertTrue(isValidFullname(fullName));
    }

    protected void assertNotValidFullName(String fullName) {
        assertFalse(isValidFullname(fullName));
    }
}
