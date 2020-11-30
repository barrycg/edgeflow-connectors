package com.edgeactor.edgeflow.common.util;


import org.apache.spark.util.jdbc.PostgresUtils;
import org.apache.spark.sql.*;

import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.spark.sql.functions.max;


public class DataframeUtils {

    public static String getMaxValueAsString(Dataset<Row> ds, String column) {
        return ds.agg(max(ds.col(column))).as(Encoders.STRING()).collectAsList().get(0);
    }

    public static Timestamp getMaxValueAsTimestamp(Dataset<Row> ds, String column) {
        return ds.agg(max(ds.col(column))).as(Encoders.TIMESTAMP()).collectAsList().get(0);
    }

    public static void nonTransactionalCopy(Dataset<Row> df, String tableName, String url, Properties properties ){

        // 1.  write the df to the local temporary file.

        // 2.  copy to target table from the temporary file.

        PostgresUtils.nonTransactionalCopy(  df, df.schema(), tableName, url, properties );
    }

}
