package com.edgeactor.edgeflow.connect;


import com.edgeactor.edgeflow.common.util.TestHelper;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMockit.class)
public class PipelineTest {

    private TestHelper testHelper = new TestHelper();

    @Test
    public void runPostgreSqlBulkPipeline() throws Exception{
        String pipelineFile = "bulkCaseTest.conf";
        testHelper.pipelineRunner(pipelineFile);
    }

    @Test
    public void runPostgreSqlUpsertPipeline() throws Exception{
        String pipelineFile = "incrementingCaseTest.conf";
        testHelper.pipelineRunner(pipelineFile);
    }

}
