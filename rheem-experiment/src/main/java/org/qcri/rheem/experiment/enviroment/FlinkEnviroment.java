package org.qcri.rheem.experiment.enviroment;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.experiment.ExperimentExecutor;

import java.io.File;
import java.net.URI;

public class FlinkEnviroment extends EnviromentExecution<ExecutionEnvironment>{

    @Override
    public ExecutionEnvironment getEnviroment() {
        try {
            ConfigValue prop = ConfigFactory.load().getValue("flink.configuration");
            Config conf = ConfigFactory.parseFile(new File(URI.create((String) prop.unwrapped())));

            if (conf.isEmpty()) {
                return ExecutionEnvironment.getExecutionEnvironment();
            } else {

                String[] jars = new String[]{
                        ReflectionUtils.getDeclaringJar(ExperimentExecutor.class)
                };


                ExecutionEnvironment environment = ExecutionEnvironment.createRemoteEnvironment(
                        conf.getString("rest.address"),
                        conf.getInt("jobmanager.rpc.port"),
                        jars
                );

                environment.setParallelism(conf.getInt("parallelism.default"));

                return environment;
            }
        }catch (ConfigException ex){
            return ExecutionEnvironment.getExecutionEnvironment();
        }
    }
}
