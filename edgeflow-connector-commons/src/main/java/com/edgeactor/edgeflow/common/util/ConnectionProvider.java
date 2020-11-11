package com.edgeactor.edgeflow.common.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider {

    Connection buildConnection() throws SQLException;

    void close();
}
