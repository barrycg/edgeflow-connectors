package com.edgeactor.edgeflow.common.dialect;


import com.edgeactor.edgeflow.common.util.ConnectionProvider;
import com.edgeactor.edgeflow.common.util.OffsetInfo;
import com.typesafe.config.Config;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Some;

import java.sql.SQLException;
import java.util.Properties;

import com.edgeactor.edgeflow.common.spark.util.SparkJdbcUtils;

public interface DatabaseDialect extends ConnectionProvider {


    DatabaseDialect create(Config config);

    /**
     * Return the name of the dialect.
     *
     * @return the dialect's name; never null
     */

    String name();

    OffsetInfo getLastOffset(
            OffsetInfo offsetInfo
    ) throws SQLException;


    void setLastOffset(
            OffsetInfo offsetInfo
    ) throws SQLException;


    String buildUpsertQueryStatement(
            String query,
            OffsetInfo offsetInfo
    );

    @Override
    void close();

    static String getTempTableName(String tableName, String prefix, String suffix) {
        String schema = null;
        String tempTable = null;
        if (tableName.contains(".")) {
            schema = StringUtils.split(tableName, '.')[0];
            tableName = StringUtils.split(tableName, '.')[1];
        }

        if (prefix != null) {
            if (prefix.endsWith(".")) {
                tempTable = prefix + tableName;
            } else {
                if (schema != null){
                    tempTable = schema + "." + prefix + tableName;
                }else {
                    tempTable = prefix + tableName;
                }
            }
        } else {
            tempTable = tableName;
        }
        if (suffix != null) {
            return tempTable + suffix;
        } else {
            return tempTable;
        }
    }

    // 数据库内部通过 SELECT 更新
    static void internalUpdateBySelectTable(StructType schema, String url, String sinkTable, String sourceTable, String keyCols, Properties properties) {

        Option<String> _keyCols = new Some<>(keyCols);

        SparkJdbcUtils.internalUpdateBySelectTable(schema,
                sinkTable,
                sourceTable,
                _keyCols,
                url,
                properties);

    }

    // 数据库内部通过 SELECT 插入
    static void internalInsertBySelectTable(StructType schema, String url, String sinkTable, String sourceTable, String keyCols, Properties properties) {

        Option<String> _keyCols = new Some<>(keyCols);
        SparkJdbcUtils.internalInsertBySelectTable(schema,
                sinkTable,
                sourceTable,
                _keyCols,
                url,
                properties);
    }


    static void upsert(Dataset<Row> df, String idCol, String url, String tableName, Properties properties, SaveMode mode, Integer batchSize) {

        Option<String> _idCol = new Some<>(idCol);
        SparkJdbcUtils.upsert(df, _idCol, url, tableName, properties, mode, batchSize);
    }


    static boolean tableExists(String url, String table, Properties properties) {
        return SparkJdbcUtils.tableExists(url, table, properties);
    }

    static void createTable(Dataset<Row> df,
                                   String keyCols,
                                   String url,
                                   String table,
                                   Properties properties,
                                   Boolean forceWhenExists) {
        Option<String> _keyCols = new Some<>(keyCols);
        SparkJdbcUtils.createTable(df, _keyCols, url, table, properties, forceWhenExists);
    }

}
