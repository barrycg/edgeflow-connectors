package com.edgeactor.edgeflow.connect;


import com.cloudera.labs.envelope.run.Runner;
import com.edgeactor.edgeflow.common.dialect.DatabaseDialect;
import com.edgeactor.edgeflow.common.dialect.DatabaseDialects;
import com.edgeactor.edgeflow.common.util.TestHelper;
import com.typesafe.config.Config;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.Connection;


@RunWith(JMockit.class)
public class DialectTest {

    @Test
    public void runPostgreSqlDialect() throws Exception{
        String pipelineFile = "caseTest.conf";
        Config config = TestHelper.getConfig(pipelineFile);

        String stepName = "dim_bi_dept";
        config = config.getConfig(Runner.STEPS_SECTION_CONFIG).getConfig(stepName).getConfig("input");

        DatabaseDialect dialect = DatabaseDialects.create("PostgreSQLDatabaseDialect",config);
        Connection connection = dialect.getConnection();
        System.out.println("ending ");
    }
}
