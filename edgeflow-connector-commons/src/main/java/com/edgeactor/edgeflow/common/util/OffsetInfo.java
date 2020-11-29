package com.edgeactor.edgeflow.common.util;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Just like jdbcInfo, this class is about offset management.
 */
public class OffsetInfo{

    private static final Logger LOG = LoggerFactory.getLogger(JdbcInfo.class);

    private String timestampColumn;
    private String incrementingColumn;

    private String namespace;
    private String topic;
    private String timestampValue;
    private String incrementingValue;

    public OffsetInfo(Config config){
        if( config.hasPath(ConnectorConfig.CONFIG_OFFSETS_TIMESTAMP_COLUMN) ){
            timestampColumn = config.getString( ConnectorConfig.CONFIG_OFFSETS_TIMESTAMP_COLUMN);
        }
        if( config.hasPath(ConnectorConfig.CONFIG_OFFSETS_INCREMENTING_COLUMN) ){
            incrementingColumn = config.getString( ConnectorConfig.CONFIG_OFFSETS_INCREMENTING_COLUMN);
        }

        if(timestampColumn == null && timestampColumn == null ){
            String message = "Config error:" +  ConnectorConfig.CONFIG_OFFSETS_TIMESTAMP_COLUMN
                    +" and "+ ConnectorConfig.CONFIG_OFFSETS_INCREMENTING_COLUMN + " could not be null at the same time!";
            LOG.error(message);
            throw  new IllegalArgumentException( message );
        }

        if (config.hasPath(ConnectorConfig.CONFIG_OFFSETS_NAMESPACE)){
            namespace = config.getString(ConnectorConfig.CONFIG_OFFSETS_NAMESPACE);
        }
        if (config.hasPath(ConnectorConfig.CONFIG_OFFSETS_TOPIC)){
            topic = config.getString(ConnectorConfig.CONFIG_OFFSETS_TOPIC);
        }

    }

    public String getTimestampColumn(){ return  timestampColumn; }

    public String getIncrementingColumn(){ return  incrementingColumn; }

    public String getNamespace(){ return  namespace; }

    public String getTopic(){ return  topic; }

    public String getTimestampValue() {
        return timestampValue;
    }

    public void setTimestampValue(String timestampValue) {
        this.timestampValue = timestampValue;
    }

    public String getIncrementingValue() {
        return incrementingValue;
    }

    public void setIncrementingValue(String incrementingValue) {
        this.incrementingValue = incrementingValue;
    }
}
