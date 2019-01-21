package org.qcri.rheem.experiment.word2nvec;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.simword.SparseVector;
import org.qcri.rheem.experiment.simwords.SimWordsFlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.HashMap;
import java.util.Map;

final public class Word2NVecFlinkImplementation extends FlinkImplementation {
    public Word2NVecFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter)parameters.getParameter("input")).getPath();
        String output = ((FileResult)results.getContainerOfResult("output")).getPath();


        int minWordOccurrences = ((VariableParameter<Integer>)parameters.getParameter("min")).getVariable();
        int neighborhoodReach = ((VariableParameter<Integer>)parameters.getParameter("neighborhoodReach")).getVariable();

        Map<String, DataSet> firstPart = SimWordsFlinkImplementation.firstPart(this.env, input, minWordOccurrences, neighborhoodReach);
        DataSet<Tuple2<Long, String>> words_id = firstPart.get("words_id");
        DataSet<Tuple2<Integer, SparseVector>> wordVectors = firstPart.get("wordVectors");


        wordVectors
            .map(
                new RichMapFunction<Tuple2<Integer, SparseVector>, Tuple3<Integer, String, SparseVector>>() {
                    private Map<Integer, String> map;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.map = new HashMap<>();
                        getRuntimeContext()
                                .<Tuple2<Long, String>>getBroadcastVariable("words_id")
                                .forEach(
                                        tuple -> map.put(tuple.f0.intValue(), tuple.f1)
                                );
                    }

                    @Override
                    public Tuple3<Integer, String, SparseVector> map(Tuple2<Integer, SparseVector> value) throws Exception {
                        return new Tuple3<>(
                            value.f0,
                            map.getOrDefault(value.f0, "(unknown)"),
                            value.f1
                        );
                    }
                }
            ).withBroadcastSet(words_id, "words_id").returns(TypeInformation.of(new TypeHint<Tuple3<Integer, String, SparseVector>>(){}))
            .writeAsText(output);

    }
}
