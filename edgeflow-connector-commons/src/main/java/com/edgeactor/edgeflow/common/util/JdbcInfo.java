package com.edgeactor.edgeflow.common.util;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcInfo {
    /**
     * The regular expression pattern to extract the JDBC subprotocol and subname from a JDBC URL of
     * the form {@code jdbc:<subprotocol>:<subname>} where {@code subprotocol} defines the kind of
     * database connectivity mechanism that may be supported by one or more drivers. The contents and
     * syntax of the {@code subname} will depend on the subprotocol.
     *
     * <p>The subprotocol will be in group 1, and the subname will be in group 2.
     *
     */
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");
    String url;
    String username;
    String password;
    private static final Logger LOG = LoggerFactory.getLogger(JdbcInfo.class);
    public JdbcInfo( String url, String username, String password ){
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public JdbcInfo( Config config ){
        this.url = config.getString(ConnectorConfig.CONFIG_JDBC_URL);
        this.username = config.getString(ConnectorConfig.CONFIG_JDBC_USERNAME);
        this.password = config.getString(ConnectorConfig.CONFIG_JDBC_PASSWORD);
    }


    public void readConfig( Config config ){
        url = config.getString(ConnectorConfig.CONFIG_JDBC_URL);
        username = config.getString(ConnectorConfig.CONFIG_JDBC_USERNAME);
        password = config.getString(ConnectorConfig.CONFIG_JDBC_PASSWORD);
    }

    public String getUrl(){
        return url;
    }

    public String getUsername(){
        return username;
    }

    public String getPassword(){
        return password;
    }

    public Properties getProperties(){
        Properties properties = new Properties();
        properties.setProperty("user", getUsername());
        properties.setProperty("password", getPassword() );
        return properties;
    }

    static boolean isValid(String url){
        Matcher matcher = PROTOCOL_PATTERN.matcher(url);
        if (matcher.matches()) {
            LOG.info(matcher.group(1)+ matcher.group(2)+url);
            return true;
        }
        LOG.error("Not a valid JDBC URL: " + url);
        return false;
    }
}