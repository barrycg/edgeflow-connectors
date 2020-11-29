package com.edgeactor.edgeflow.connect;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.edgeactor.edgeflow.common.dialect.DatabaseDialect;
import com.edgeactor.edgeflow.common.dialect.DatabaseDialects;
import com.edgeactor.edgeflow.common.util.ConnectorConfig;
import com.edgeactor.edgeflow.common.util.DataframeUtils;
import com.edgeactor.edgeflow.common.util.JdbcInfo;
import com.edgeactor.edgeflow.common.util.OffsetInfo;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

/**
 *  适用于postgre的BatchOutput
 *  支持全量和增量两种模式, 增量对应的信息放在数据库本身。
 *  @author
 */
public class PostgreSqlOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {


    private JdbcInfo jdbcInfo;
    private Integer batchSize=100;
    private String tableName;
    private String upsertPolicy = ConnectorConfig.UPSERT_POLICY_NATIVE;
    private String upsertTempPrefix = "tmp.";
    private String keyColumns;

    OffsetInfo offsetInfo;
    DatabaseDialect offsetDialect;

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
                        upsertDataFrame(row._2);
                        break;
                    case DELETE:
                        // todo
                        break;
                    default:
                        throw new IllegalArgumentException("PostgreSqlOutput does not support mutation type: " + row._1);
                }

                if( row._1.equals(MutationType.UPSERT)){
                    saveOffset(row._2);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("PostgreSqlOutput JDBC error" + ex.getMessage() , ex);
        }
    }

    private void saveOffset(Dataset<Row> row ) throws Exception{

        String maxValueAsString = DataframeUtils.getMaxValueAsString(row, offsetInfo.getIncrementingColumn());
        Timestamp maxValueAsTimestamp = DataframeUtils.getMaxValueAsTimestamp(row, offsetInfo.getTimestampColumn());
        if( maxValueAsString != null || maxValueAsTimestamp != null) {
            offsetInfo.setTimestampValue(maxValueAsTimestamp.toString());
            offsetInfo.setIncrementingValue(maxValueAsString);
            offsetDialect.buildConnection();
            offsetDialect.setLastOffset(offsetInfo);
            offsetDialect.close();
        }
    }

    private void appendDataFrame(Dataset<Row> df) {
        df.write().mode(SaveMode.Append).option("batchsize", batchSize)
                .jdbc(jdbcInfo.getUrl(), tableName, jdbcInfo.getProperties());
    }

    private void upsertDataFrame(Dataset<Row> df) throws Exception {

        if( createIfNotExist(df) ){
            return;
        }

        switch ( upsertPolicy ){
            case ConnectorConfig.UPSERT_POLICY_UPDATE_INSERT:
                upsertDataFrameUseUpdateAndInsert(df);
                break;
            case ConnectorConfig.UPSERT_POLICY_NATIVE:
                upsertDataFrameUseNative(df);
                break;
            default:
                throw new IllegalArgumentException("PostgreSqlOutput does not support upsert type: " + upsertPolicy);
        }

    }

    /**
     *
     *  判断目标表是否存在，如不存在则直接降级为新增
     * @param df
     * @return
     * @throws Exception
     */
    private boolean createIfNotExist(Dataset<Row> df) throws Exception{
        if (StringUtils.isEmpty(keyColumns)) {
            throw new IllegalArgumentException("Upsert 模式下 key-columns 不能为空");
        }
        boolean tableExists = DatabaseDialect.tableExists(jdbcInfo.getUrl(), tableName, jdbcInfo.getProperties());

        if (!tableExists) {
            DatabaseDialect.createTable(df, keyColumns, jdbcInfo.getUrl(), tableName, jdbcInfo.getProperties(), false);
            appendDataFrame(df);
            return true;
        }else{
            return false;
        }
    }

    private void upsertDataFrameUseNative(Dataset<Row> df) {
        DatabaseDialect.upsert(df, keyColumns, jdbcInfo.getUrl(), tableName,
                                  jdbcInfo.getProperties(), SaveMode.Append, batchSize);
    }

    private void upsertDataFrameUseUpdateAndInsert(Dataset<Row> df) {
        // 1. 写入缓存表
        String tempTable = DatabaseDialect.getTempTableName(tableName, upsertTempPrefix, null);
        df.write().mode(SaveMode.Overwrite).option("batchsize", batchSize).jdbc(jdbcInfo.getUrl(),
                                                tempTable, jdbcInfo.getProperties());
        // 2. 从缓存表更新目标表
        DatabaseDialect.internalUpdateBySelectTable(df.schema(), jdbcInfo.getUrl(), tableName,
                        tempTable, keyColumns, jdbcInfo.getProperties());
        // 3. 从缓存表插入新纪录到目标表
        DatabaseDialect.internalInsertBySelectTable(df.schema(), jdbcInfo.getUrl(), tableName,
                tempTable, keyColumns, jdbcInfo.getProperties());
    }

    @Override
    public void configure(Config config) {
        jdbcInfo = new JdbcInfo(config);
        tableName = config.getString(ConnectorConfig.CONFIG_JDBC_TABLE);
        if(config.hasPath( ConnectorConfig.CONFIG_BATCH_SIZE)){
            batchSize = config.getInt(ConnectorConfig.CONFIG_BATCH_SIZE);
        }

        if(config.hasPath( ConnectorConfig.CONFIG_UPSERT_POLICY)){
            upsertPolicy = config.getString(ConnectorConfig.CONFIG_UPSERT_POLICY);
        }
        if (config.hasPath(ConnectorConfig.CONFIG_UPSERT_TEMP_PREFIX)) {
            upsertTempPrefix = config.getString(ConnectorConfig.CONFIG_UPSERT_TEMP_PREFIX);
        }

        if (config.hasPath(ConnectorConfig.CONFIG_UPSERT_KEY_COLUMNS)) {
            keyColumns = config.getString(ConnectorConfig.CONFIG_UPSERT_KEY_COLUMNS);
        }

        if (config.hasPath("offsets")) {
            offsetInfo = new OffsetInfo(config);
            try {
                offsetDialect = DatabaseDialects.create("PostgreSqlDatabaseDialect",config);
            } catch (Exception e) {
                e.printStackTrace();
                offsetDialect = null;
            }
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
                .optionalPath(ConnectorConfig.CONFIG_UPSERT_POLICY, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_UPSERT_TEMP_PREFIX, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_UPSERT_KEY_COLUMNS, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_OFFSETS_TIMESTAMP_COLUMN, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_OFFSETS_INCREMENTING_COLUMN, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_OFFSETS_NAMESPACE, ConfigValueType.STRING)
                .optionalPath(ConnectorConfig.CONFIG_OFFSETS_TOPIC, ConfigValueType.STRING)
                .build();
    }
}
