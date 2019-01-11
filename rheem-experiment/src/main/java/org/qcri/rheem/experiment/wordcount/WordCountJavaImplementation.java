package org.qcri.rheem.experiment.wordcount;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.experiment.implementations.java.JavaImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.*;


import static java.util.stream.Collectors.toMap;

final public class WordCountJavaImplementation extends JavaImplementation {

    public WordCountJavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        this.writeFile(
            ((FileResult)results.getContainerOfResult("output")).getURI(),
            this.getStreamOfFile(
                ((FileParameter) parameters.getParameter("input")).getURI()
            )
            .flatMap(line -> Arrays.stream(line.split("\\W+")))
            .filter(word -> !word.isEmpty())
            .map(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1))
            .collect(
                toMap(
                    tuple -> tuple.getField0(),
                    tuple -> tuple.getField1(),
                    (value1, value2) -> value1 + value2
                )
            ).entrySet().stream()
            .map(
                entry ->  String.format("(%s, %s)\n", entry.getKey(), entry.getValue())
            )
        );

    }
}