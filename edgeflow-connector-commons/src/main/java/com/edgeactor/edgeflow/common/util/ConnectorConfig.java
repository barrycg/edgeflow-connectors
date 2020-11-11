package com.edgeactor.edgeflow.common.util;

public class ConnectorConfig {

    public static final String CONFIG_JDBC_URL = "datasource.url";
    public static final String CONFIG_JDBC_USERNAME = "datasource.username";
    public static final String CONFIG_JDBC_PASSWORD = "datasource.password";


    /// input
    public static final String CONFIG_MODE = "mode"; // bulk,  timestamp
    public static final String RMODE_BULK = "bulk";
    public static final String RMODE_TIMESTAMP_INCREMENTING = "timestamp-incrementing";

    public static final String CONFIG_OFFSETS_TIMESTAMP_COLUMN = "offsets.timestamp-column";
    public static final String CONFIG_OFFSETS_INCREMENTING_COLUMN = "offsets.incrementing-column";
    public static final String CONFIG_OFFSETS_NAMESPACE = "offsets.namespace";
    public static final String CONFIG_OFFSETS_TOPIC = "offsets.topic";

    public static final String CONFIG_FETCH_SIZE = "fetch-size";
    public static final String CONFIG_JDBC_QUERY = "query";
    public static final String CONFIG_JDBC_TABLE = "table";

    public static final String CONFIG_BATCH_SIZE = "batch-size";


    /// output
    public static final String CONFIG_UPSERT_POLICY = "upsert.policy";
    public static final String UPSERT_POLICY_NATIVE = "native";
    public static final String UPSERT_POLICY_DELETE_APPEND = "delete-append";
    public static final String UPSERT_POLICY_UPDATE_INSERT = "update-insert";

    public static final String CONFIG_UPSERT_KEY_COLUMNS = "upsert.key-columns";
    public static final String CONFIG_UPSERT_TEMP_PREFIX = "upsert.temp-prefix";

}
