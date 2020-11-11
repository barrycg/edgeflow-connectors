package com.edgeactor.edgeflow.connect;


import com.edgeactor.edgeflow.common.dialect.DatabaseDialect;
import com.edgeactor.edgeflow.common.util.ExpressionBuilder;
import com.edgeactor.edgeflow.common.util.JdbcInfo;
import com.edgeactor.edgeflow.common.util.OffsetInfo;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.Collection;


public class PostgreSqlDatabaseDialect implements DatabaseDialect {

    private static Logger LOG = LoggerFactory.getLogger(PostgreSqlDatabaseDialect.class);
    private JdbcInfo jdbcInfo;
    private Connection connection;

    public PostgreSqlDatabaseDialect(){}

    private PostgreSqlDatabaseDialect(Config config){
        jdbcInfo = new JdbcInfo(config);
    }

    @Override
    public DatabaseDialect create(Config config) {
        return  new PostgreSqlDatabaseDialect(config);
    }

    @Override
    public String name() {
        return "PostgreSqlDatabaseDialect";
    }

    @Override
    public OffsetInfo getLastOffset(OffsetInfo offsetInfo) throws SQLException {

        String querySql = "select id, namespace, topic, offset_timestamp, offset_incrementing, created_by, created_at from edgeflow_offset_store " +
                                    "where namespace = \'"+offsetInfo.getNamespace() +
                                    "\' and topic = \'" +offsetInfo.getTopic()+"\'"
                                    +" order by created_at desc limit 1";
        Statement stmt = this.connection.createStatement();
        ResultSet res = stmt.executeQuery(querySql);
        if ( res.next() ) {
            offsetInfo.setIncrementingValue(res.getString("offset_incrementing"));
            offsetInfo.setTimestampValue(res.getString("offset_timestamp"));
        }
        return offsetInfo;
    }

    @Override
    public void setLastOffset(OffsetInfo offsetInfo) throws SQLException {

        Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());

        String insertSql = "insert into public.edgeflow_offset_store( namespace, topic,offset_timestamp, offset_incrementing, created_by, created_at) values("
                +"\'"+ offsetInfo.getNamespace() + "\',"
                +"\'"+ offsetInfo.getTopic() + "\',"
                +"\'"+ offsetInfo.getTimestampValue() + "\',"
                +"\'"+ offsetInfo.getIncrementingValue() + "\',"
                +"\'"+ name() + "\',"
                +"\'"+timestamp + "\')";

        Statement stmt = this.connection.createStatement();
        stmt.executeUpdate(insertSql);
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
    public String buildUpsertQueryStatement(String query, OffsetInfo offsetInfo) {

        String res = null;
        if( offsetInfo.getTimestampColumn() != null && offsetInfo.getTimestampValue() != null ){
            // append timestamp
            res = "select * from "+ ExpressionBuilder.wrapQueryAsTable( query, "DF9999" )
                + " where DF9999." + offsetInfo.getTimestampColumn() + " >= \'" + offsetInfo.getTimestampValue() + "\'";
        }

        if( offsetInfo.getIncrementingValue() != null &&offsetInfo.getIncrementingValue() != null){
            if( res == null){
                res = "select * from "+ ExpressionBuilder.wrapQueryAsTable( query, "DF9999" )
                        + "where DF9999." + offsetInfo.getIncrementingColumn() + " >= \'"
                        + offsetInfo.getIncrementingValue() + "\'";
            }else{
                res += "and DF9999." + offsetInfo.getIncrementingColumn() + " >= \'"
                        + offsetInfo.getIncrementingValue() + "\'";
            }
        }

        if( res != null ){
            res = ExpressionBuilder.wrapQueryAsTable( res, "DF8888" );
        }else{
            res = ExpressionBuilder.wrapQueryAsTable( query, "DF8888" );
        }

        return res;
    }

    @Override
    public Connection buildConnection() throws SQLException {
        if( connection == null){
            DriverManager.setLoginTimeout(40);
            connection = DriverManager.getConnection( jdbcInfo.getUrl(), jdbcInfo.getProperties());
        }
        return connection;
    }

    @Override
    public void close() {
        if( connection != null ){
            try {
               connection.close();
            }catch (SQLException ex) {
                LOG.error( ex.getMessage());
            }
        }
    }
}
