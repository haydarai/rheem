package org.qcri.rheem.experiment.wordcount.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.qcri.rheem.experiment.Implementation;
import org.qcri.rheem.experiment.enviroment.FlinkEnviroment;
import org.qcri.rheem.utils.parameters.RheemParameters;
import org.qcri.rheem.utils.parameters.type.FileParameter;
import org.qcri.rheem.utils.results.RheemResults;
import org.qcri.rheem.utils.results.type.FileResult;
import org.qcri.rheem.utils.udf.UDFs;

import java.util.Arrays;

public class FlinkImplementation extends Implementation {

    public FlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }


    @Override
    public RheemResults executePlan() {
        ExecutionEnvironment env = ((FlinkEnviroment)this.enviroment).getEnviroment();

        DataSink<Tuple2<String, Integer>> output = env.readTextFile(((FileParameter) parameters.getParameter("input")).getPath())
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        Arrays.stream(line.split("\\W+")).forEach(
                                collector::collect
                        );
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return !value.isEmpty();
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value.toLowerCase(), 1);
                    }
                })
                .groupBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .writeAsText(((FileResult) results.getContainerOfResult("output")).getPath());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return this.results;
    }
}
