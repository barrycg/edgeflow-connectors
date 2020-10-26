package com.edgeactor.edgeflow.connect;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlInput implements BatchInput, ProvidesAlias, ProvidesValidations {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static final String CONFIG_URL = "datasource.url";
    public static final String CONFIG_USERNAME = "datasource.username";
    public static final String CONFIG_PASSWORD = "datasource.password";
    public static final String CONFIG_TABLE_OR_QUERY = "table-or-query";
    public static final String CONFIG_MODE = "mode"; // bulk, id, timestamp, id-timestamp
    public static final String CONFIG_OFFSET_MANAGE = "offset.manage"; // true / false
    public static final String CONFIG_OFFSET_OUTPUT_PREFIX = "offset.output";
    public static final String CONFIG_OFFSET_ID = "offset.cols.id";
    public static final String CONFIG_OFFSET_TIMESTAMP = "offset.cols.timestamp";
    public static final String CONFIG_PARTITION_COLUMN = "partition.column";
    public static final String CONFIG_PARTITION_LOWER_BOUND = "partition.lower-bound";
    public static final String CONFIG_PARTITION_UPPER_BOUND = "partition.upper-bound";
    public static final String CONFIG_PARTITION_NUM = "partition.num";
    public static final String CONFIG_PARAMETER_PREFIX = "parameter";


    private String url;
    private String username;
    private String password;
    //    private String driver;
    private String tableOrQuery;
    private String mode;
    //    private OffsetStore offsetStore;
    private String topic;
    private String idCol;
    private String timestampCol;
    private String partitionCol;
    private String partitionLower;
    private String partitionUpper;
    private Integer partitionNum;
    private Config paramConfig = ConfigFactory.empty();

    @Override
    public String getAlias() {
        return "mysql";
    }

    @Override
    public Dataset<Row> read() throws Exception {
        return null;
    }

    @Override
    public void configure(Config config) {

    }

    @Override
    public Validations getValidations() {
        return null;
    }
}
