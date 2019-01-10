package org.qcri.rheem.experiment.wordcount;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

final public class WordCountSparkImplementation extends SparkImplementation {

    public WordCountSparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input_file = ((FileParameter)parameters.getParameter("input")).getPath();
        this.sparkContext.textFile(input_file)
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterator<String> call(String line) throws Exception {
                    return Arrays.asList(line.split("\\W+")).iterator();
                }
            })
            .filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return ! v1.isEmpty();
                }
            })
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return  new Tuple2<>(s.toLowerCase(), 1);
                }
            })
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            })
            .saveAsTextFile(
                    ((FileResult)results.getContainerOfResult("output")).getPath()
            );
    }
}
