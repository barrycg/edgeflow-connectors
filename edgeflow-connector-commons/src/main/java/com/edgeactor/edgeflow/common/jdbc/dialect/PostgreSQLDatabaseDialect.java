package com.edgeactor.edgeflow.common.jdbc.dialect;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Properties;

public class PostgreSQLDatabaseDialect implements  DatabaseDialect {


        private String jdbcUrl;

        @Override
        public DatabaseDialect create() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection connection, String query) throws SQLException {
            return null;
        }

        @Override
        public String buildDropTableStatement(String table, boolean isExists) {
            return null;
    }

    @Override
    public String buildCreateTableStatement(String table, Collection<String> fields) {
        return null;
    }

    @Override
    public String buildInsertStatement(String table, Collection<String> keyColumns, Collection<String> nonKeyColumns) {
        return null;
    }

    @Override
    public String buildUpdateStatement(String table, Collection<String> keyColumns, Collection<String> nonKeyColumns) {
        return null;
    }

    @Override
    public String buildUpsertQueryStatement(String table, Collection<String> keyColumns, Collection<String> nonKeyColumns) {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        String username = "";
        String dbPassword = "";
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (dbPassword != null) {
            //properties.setProperty("password", dbPassword.value());
            properties.setProperty("password", dbPassword);
        }

        //properties = addConnectionProperties(properties);

        DriverManager.setLoginTimeout(40);
        Connection connection = DriverManager.getConnection(jdbcUrl, properties);
//        if (jdbcDriverInfo == null) {
//            jdbcDriverInfo = createJdbcDriverInfo(connection);
//        }
//        connections.add(connection);
        return connection;
    }

    @Override
    public void close() {

    }
}
