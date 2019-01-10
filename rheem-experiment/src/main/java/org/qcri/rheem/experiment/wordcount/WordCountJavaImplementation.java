package org.qcri.rheem.experiment.wordcount;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.experiment.implementations.java.JavaImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


import static java.util.stream.Collectors.toMap;

final public class WordCountJavaImplementation extends JavaImplementation {

    public WordCountJavaImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        try {
            Files.write(
                Paths.get(((FileResult)results.getContainerOfResult("output")).getPath()),
                Files.lines(
                    Paths.get(
                        ((FileParameter) parameters.getParameter("input")).getPath()
                    )
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
                    entry ->  String.format("%s %d", entry.getKey(), entry.getValue())
                ).collect(
                        Collectors.toList()
                )
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}