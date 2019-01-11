package org.qcri.rheem.experiment.tpch.query1;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.experiment.implementations.java.JavaImplementation;
import org.qcri.rheem.experiment.tpch.entities.Entity;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.Arrays;

import static java.util.stream.Collectors.toMap;

final public class Q1JavaImplementation extends JavaImplementation {
    public Q1JavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String start_string = ((VariableParameter<String>)parameters.getParameter("date_min")).getVariable();
        int delta = ((VariableParameter<Integer>)parameters.getParameter("delta")).getVariable();

        int start_date = Entity.dateValue(start_string);

/*
        this.writeFile(
            ((FileResult)results.getContainerOfResult("output")).getURI(),
            this.getStreamOfFile(
                ((FileParameter) parameters.getParameter("lineitem")).getURI()
            ).map(line -> new Entity(line))
            .filter(entity -> {
                entity.parseDate(10) <= start_date - delta
            })
            .map(entity -> {
                new Entity(entity.get)
            })
        );*/
    }
}
