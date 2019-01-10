package org.qcri.rheem.experiment.utils.results;

import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.utils.results.type.RheemResult;

import java.io.Serializable;
import java.util.HashMap;

public class RheemResults implements Serializable {

    private HashMap<String, RheemResult> parameters;

    public RheemResults() {
        this.parameters = new HashMap<>();
    }

    public void addContainerOfResult(String name, RheemResult parameter){
        this.parameters.put(name, parameter);
    }

    public RheemResult getContainerOfResult(String name){
        if( ! this.parameters.containsKey(name) ){
            throw new ExperimentException("The parameter not exist with the name "+name);
        }
        return this.parameters.get(name);
    }

    public void show(){

    }
}
