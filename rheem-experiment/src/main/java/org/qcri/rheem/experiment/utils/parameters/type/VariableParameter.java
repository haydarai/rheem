package org.qcri.rheem.experiment.utils.parameters.type;

public class VariableParameter<T> implements RheemParameter {

    T variable;

    public VariableParameter(T variable) {
        this.variable = variable;
    }

    public T getVariable() {
        return variable;
    }
}
