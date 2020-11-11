package com.edgeactor.edgeflow.common.util;


import org.apache.spark.sql.*;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.max;


public class DataframeUtils {

    public static String getMaxValueAsString(Dataset<Row> ds, String column) {
        return ds.agg(max(ds.col(column))).as(Encoders.STRING()).collectAsList().get(0);
    }

    public static Timestamp getMaxValueAsTimestamp(Dataset<Row> ds, String column) {
        return ds.agg(max(ds.col(column))).as(Encoders.TIMESTAMP()).collectAsList().get(0);
    }

}
