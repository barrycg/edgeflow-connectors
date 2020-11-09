package com.edgeactor.edgeflow.common.util;

/**
 * Used for building SQL Expressions for different database.
 */

public class ExpressionBuilder {

    public static String wrapQueryAsTable(String query, String alias){
        return "(" +query +") " + alias;
    }

}
