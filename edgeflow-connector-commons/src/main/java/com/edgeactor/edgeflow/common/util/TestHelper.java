package com.edgeactor.edgeflow.common.util;

import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;

import java.net.URL;
import java.util.Objects;

/**
 *  Used to simplify configuration-file-related test.
 */
public class TestHelper {

    public void pipelineRunner(String pipelineFile) throws Exception {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(pipelineFile);

        String configPath = Objects.requireNonNull(resource.getPath());

        Config config = ConfigUtils.configFromPath(configPath);
        config = ConfigUtils.applySubstitutions(config);

        Runner runner = new Runner();
        runner.run(config);
    }

    public static Config getConfig(String pipelineFile) throws Exception{
        ClassLoader classLoader = TestHelper.class.getClassLoader();
        URL resource = classLoader.getResource(pipelineFile);
        String configPath = Objects.requireNonNull(resource.getPath());
        Config config = ConfigUtils.configFromPath(configPath);
        config = ConfigUtils.applySubstitutions(config);
        return config;
    }
}
