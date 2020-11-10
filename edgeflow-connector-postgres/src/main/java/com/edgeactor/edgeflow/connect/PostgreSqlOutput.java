package com.edgeactor.edgeflow.connect;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.edgeactor.edgeflow.common.util.ConnectorConfig;
import com.edgeactor.edgeflow.common.util.JdbcInfo;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public class PostgreSqlOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {


    private JdbcInfo jdbcInfo;
    private Integer batchSize=100;
    private String tableName;

    @Override
    public String getAlias() {
        return "postgresql-output";
    }

    @Override
    public Set<MutationType> getSupportedBulkMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT, MutationType.UPDATE, MutationType.DELETE,MutationType.UPSERT);
    }

    @Override
    public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> rows ) {
        try {

            for (Tuple2<MutationType, Dataset<Row>> row: rows) {
                switch (row._1) {
                    case INSERT:
                        appendDataFrame(row._2);
                        break;
                    case OVERWRITE:
                        // todo
                        break;
                    case UPSERT:
                        // todo
                        break;
                    case DELETE:
                        // todo
                        break;
                    default:
                        throw new IllegalArgumentException("PostgreSqlOutput does not support mutation type: " + row._1);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("PostgreSqlOutput JDBC error" + ex.getMessage() , ex);
        }
    }

    private void appendDataFrame(Dataset<Row> df) {
        df.write().mode(SaveMode.Append).option("batchsize", batchSize)
                .jdbc(jdbcInfo.getUrl(), tableName, jdbcInfo.getProperties());
    }

    @Override
    public void configure(Config config) {
        jdbcInfo = new JdbcInfo(config);
        tableName = config.getString(ConnectorConfig.CONFIG_JDBC_TABLE);
        if(config.hasPath( ConnectorConfig.CONFIG_BATCH_SIZE)){
            batchSize = config.getInt(ConnectorConfig.CONFIG_BATCH_SIZE);
        }
    }

    @Override
    public Validations getValidations() {
        return Validations.builder()
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_URL, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_USERNAME, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_PASSWORD, ConfigValueType.STRING)
                .mandatoryPath(ConnectorConfig.CONFIG_JDBC_TABLE, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_BATCH_SIZE, ConfigValueType.NUMBER)
                .build();
    }
}
