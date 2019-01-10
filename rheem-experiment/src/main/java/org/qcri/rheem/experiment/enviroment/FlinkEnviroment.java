package org.qcri.rheem.experiment.enviroment;

import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkEnviroment extends EnviromentExecution<ExecutionEnvironment>{

    @Override
    public ExecutionEnvironment getEnviroment() {
        return ExecutionEnvironment.getExecutionEnvironment();
    }
}
