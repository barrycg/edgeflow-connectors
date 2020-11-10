package com.edgeactor.edgeflow.connect;


import com.edgeactor.edgeflow.common.dialect.DatabaseDialect;
import com.edgeactor.edgeflow.common.util.JdbcInfo;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

public class PostgreSQLDatabaseDialect implements DatabaseDialect {

    private static Logger LOG = LoggerFactory.getLogger(PostgreSQLDatabaseDialect.class);
    private JdbcInfo jdbcInfo;
    private Connection connection;

    public PostgreSQLDatabaseDialect(){}

    private PostgreSQLDatabaseDialect(Config config){
        jdbcInfo = new JdbcInfo(config);
    }

    @Override
    public DatabaseDialect create(Config config) {
        return  new PostgreSQLDatabaseDialect(config);
    }

    @Override
    public String name() {
        return "PostgreSQLDatabaseDialect";
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
        //properties = addConnectionProperties(properties);
        if( connection == null){
            DriverManager.setLoginTimeout(40);
            connection = DriverManager.getConnection( jdbcInfo.getUrl(), jdbcInfo.getProperties());
        }
        return connection;
    }

    @Override
    public void close() {
        if( connection !=null ){
            try {
               connection.close();
            }catch (SQLException ex) {
                LOG.error( ex.getMessage());
            }
        }
    }
}
