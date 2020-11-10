package com.edgeactor.edgeflow.connect;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.input.BatchInput;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.edgeactor.edgeflow.common.util.ConnectorConfig;
import com.edgeactor.edgeflow.common.util.ExpressionBuilder;
import com.edgeactor.edgeflow.common.util.JdbcInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class PostgreSqlInput implements BatchInput, ProvidesAlias, ProvidesValidations {

    private static final Logger LOG = LoggerFactory.getLogger(PostgreSqlInput.class);

    String mode;
    Integer fetchSize = 100;
    JdbcInfo jdbcInfo;
    String query;


    @Override
    public String getAlias() {
        return "postgresql-input";
    }

    @Override
    public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> read() throws Exception {

        Dataset<Row> df = null;
        switch ( mode ){
            case ConnectorConfig.RMODE_BULK:
                df = bulkRead();
                break;
            case ConnectorConfig.RMODE_INCREMENTING:
                //todo
                break;
            default:
                throw new IllegalArgumentException("mode 类型: " + mode + "不支持");
        }

        return df;
    }


    private Dataset<Row> bulkRead(){
         DataFrameReader reader = Contexts.getSparkSession().read().option("fetchsize", fetchSize.toString() )
                .option("pushDownPredicate", true).option("driver","org.postgresql.Driver");

        String querySql = ExpressionBuilder.wrapQueryAsTable(query, "DF9999");
        Dataset<Row> jdbc = reader.jdbc(jdbcInfo.getUrl(), querySql, jdbcInfo.getProperties());
        return jdbc;
    }

    @Override
    public void configure(Config config) {

        jdbcInfo = new JdbcInfo(config);
        mode = config.getString( ConnectorConfig.CONFIG_MODE );
        if( config.hasPath(ConnectorConfig.CONFIG_FETCH_SIZE) ){
            fetchSize = config.getInt( ConnectorConfig.CONFIG_FETCH_SIZE);
        }
        if( config.hasPath(ConnectorConfig.CONFIG_JDBC_QUERY) ){
            query = config.getString( ConnectorConfig.CONFIG_JDBC_QUERY);
        }
    }

    @Override
    public Validations getValidations() {
        return Validations.builder()
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_URL, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_USERNAME, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_PASSWORD, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_MODE, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_FETCH_SIZE,ConfigValueType.NUMBER)
                .optionalPath(ConnectorConfig.CONFIG_JDBC_QUERY, ConfigValueType.STRING)
                .build();
    }
}
