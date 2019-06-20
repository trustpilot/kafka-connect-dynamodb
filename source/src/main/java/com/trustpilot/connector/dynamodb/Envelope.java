package com.trustpilot.connector.dynamodb;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
public final class Envelope {
    /**
     * The constants for the values for the {@link FieldName#OPERATION operation} field in the message envelope.
     */
    public enum Operation {
        /**
         * The operation that read the current state of a record, most typically during snapshots.
         */
        READ("r"),
        /**
         * An operation that resulted in a new record being created in the source.
         */
        CREATE("c"),
        /**
         * An operation that resulted in an existing record being updated in the source.
         */
        UPDATE("u"),
        /**
         * An operation that resulted in an existing record being removed from or deleted in the source.
         */
        DELETE("d");
        private final String code;

        Operation(String code) {
            this.code = code;
        }

        public static Operation forCode(String code) {
            for (Operation op : Operation.values()) {
                if (op.code().equalsIgnoreCase(code)) {
                    return op;
                }
            }
            return null;
        }

        public String code() {
            return code;
        }
    }

    /**
     * The constants for the names of the fields in the message envelope.
     */
    public static final class FieldName {
        public static final String VERSION = "version";

        /**
         * The {@code after} field is used to store the state of a record after an operation.
         */
        public static final String DOCUMENT = "document";
        /**
         * The {@code op} field is used to store the kind of operation on a record.
         */
        public static final String OPERATION = "op";
        /**
         * The {@code origin} field is used to store the information about the source of a record, including the
         * Kafka Connect partition and offset information.
         */
        public static final String SOURCE = "source";
        /**
         * The {@code ts_ms} field is used to store the information about the local time at which the connector
         * processed/generated the event. The timestamp values are the number of milliseconds past epoch (January 1, 1970), and
         * determined by the {@link System#currentTimeMillis() JVM current time in milliseconds}. Note that the <em>accuracy</em>
         * of the timestamp value depends on the JVM's system clock and all of its assumptions, limitations, conditions, and
         * variations.
         */
        public static final String TIMESTAMP = "ts_ms";
    }

}
