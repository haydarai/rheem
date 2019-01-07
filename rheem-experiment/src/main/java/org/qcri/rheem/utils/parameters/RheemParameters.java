package org.qcri.rheem.utils.parameters;

import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.utils.parameters.type.RheemParameter;

import java.util.HashMap;

public class RheemParameters {

    private HashMap<String, RheemParameter> parameters;

    public RheemParameters() {
         this.parameters = new HashMap<>();
    }

    public void addParameter(String name, RheemParameter parameter){
        this.parameters.put(name, parameter);
    }

    public RheemParameter getParameter(String name){
        if( ! this.parameters.containsKey(name) ){
            throw new ExperimentException("The parameter not exist with the name "+name);
        }
        return this.parameters.get(name);
    }
}
