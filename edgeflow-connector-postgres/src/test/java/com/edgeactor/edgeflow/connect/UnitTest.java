package com.edgeactor.edgeflow.connect;


import com.edgeactor.edgeflow.common.util.TestHelper;
import mockit.integration.junit4.JMockit;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JMockit.class)
public class UnitTest {

    private TestHelper testHelper = new TestHelper();

    @Test
    public void runPostgreSqlInput() throws Exception{

        String pipelineFile = "caseTest.conf";
        testHelper.pipelineRunner(pipelineFile);

    }

}
