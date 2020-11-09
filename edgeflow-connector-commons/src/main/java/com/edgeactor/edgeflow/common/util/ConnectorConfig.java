package com.edgeactor.edgeflow.common.util;

public class ConnectorConfig {

    public static final String CONFIG_JDBC_URL = "datasource.url";
    public static final String CONFIG_JDBC_USERNAME = "datasource.username";
    public static final String CONFIG_JDBC_PASSWORD = "datasource.password";


    /// input
    public static final String CONFIG_MODE = "mode"; // bulk,  timestamp
    public static final String RMODE_TIMESTAMP_INCREMENTING = "timestamp-incrementing";
    public static final String RMODE_TIMESTAMP = "timestamp";
    public static final String RMODE_INCREMENTING = "incrementing";
    public static final String RMODE_BULK = "bulk";

    public static final String CONFIG_FETCH_SIZE = "fetch-size";
    public static final String CONFIG_JDBC_QUERY = "query";
}
