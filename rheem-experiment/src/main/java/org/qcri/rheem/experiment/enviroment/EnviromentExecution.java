package org.qcri.rheem.experiment.enviroment;

import java.io.Serializable;

public abstract class EnviromentExecution<T> implements Serializable {

    public abstract T getEnviroment();

}
