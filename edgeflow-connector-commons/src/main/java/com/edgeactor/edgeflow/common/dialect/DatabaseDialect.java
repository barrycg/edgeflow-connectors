package com.edgeactor.edgeflow.common.dialect;


import com.edgeactor.edgeflow.common.util.ConnectionProvider;
import com.typesafe.config.Config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

public interface DatabaseDialect extends ConnectionProvider {


    DatabaseDialect create(Config config);

    /**
     * Return the name of the dialect.
     *
     * @return the dialect's name; never null
     */

    String name();


    PreparedStatement createPreparedStatement(
            Connection connection,
            String query
    ) throws SQLException;


    String buildDropTableStatement(String table, boolean isExists);


    String buildCreateTableStatement(String table, Collection<String> fields);


    String buildInsertStatement(
            String table,
            Collection<String> keyColumns,
            Collection<String> nonKeyColumns
    );

    String buildUpdateStatement(
            String table,
            Collection<String> keyColumns,
            Collection<String> nonKeyColumns
    );


    String buildUpsertQueryStatement(
            String table,
            Collection<String> keyColumns,
            Collection<String> nonKeyColumns
    );

    @Override
    void close();

}
