package org.qcri.rheem.experiment.sgd;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;

import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.experiment.ExperimentException;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

import java.util.Arrays;
import java.util.List;

final public class SGDFlinkImplementation extends FlinkImplementation {
    public SGDFlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String value = ((FileParameter) parameters.getParameter("input")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        int features = ((VariableParameter<Integer>)parameters.getParameter("features")).getVariable();
        int sample_size = ((VariableParameter<Integer>)parameters.getParameter("sample_size")).getVariable();
        int max_iterations = ((VariableParameter<Integer>)parameters.getParameter("max_iterations")).getVariable();
        double accuracy = ((VariableParameter<Double>)parameters.getParameter("accuracy")).getVariable();

        double[] weights = new double[features];

        DataSet<double[]> weights_set = this.env.fromElements(weights);


        DataSet<double[]> transformBuilder = this.env.readTextFile(value)
                .mapPartition(new TransformPerPartitionFlink(features));

        DataSet<double[]> weights_transitive = weights_set;

        int iteration = 0;

        boolean condition = true;
        try {
            do {
                weights_set = weights_transitive;

                List<double[]> weights_tmp = DataSetUtils.sampleWithSize(transformBuilder, false, sample_size)
                        .mapPartition(new ComputeLogisticGradientPerPartitionFlink(features))
                        .withBroadcastSet(weights_set, "weights")
                        .reduce(new ReduceFunction<double[]>() {
                            @Override
                            public double[] reduce(double[] value1, double[] value2) throws Exception {

                                if (value2 == null) //samples came from one partition only
                                    return value1;

                                if (value1 == null) //samples came from one partition only
                                    return value2;

                                double[] sum = new double[value1.length];
                                sum[0] = value1[0] + value2[0]; //count
                                for (int i = 1; i < value1.length; i++)
                                    sum[i] = value1[i] + value2[i];

                                return sum;
                            }
                        })
                        .map(new RichMapFunction<double[], double[]>() {

                            double[] weights;
                            int current_iteration;

                            double stepSize = 1;
                            double regulizer = 0;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                this.weights = getRuntimeContext().<double[]>getBroadcastVariable("weights").get(0);
                                this.current_iteration = getRuntimeContext().<Integer>getBroadcastVariable("iterations").get(0);
                            }

                            @Override
                            public double[] map(double[] value) throws Exception {
                                double count = value[0];
                                double alpha = (stepSize / (current_iteration + 1));
                                double[] newWeights = new double[weights.length];
                                for (int j = 0; j < weights.length; j++) {
                                    newWeights[j] = (1 - alpha * regulizer) * weights[j] - alpha * (1.0 / count) * value[j + 1];
                                }
                                return newWeights;
                            }
                        })
                        .withBroadcastSet(weights_set, "weights")
                        .withBroadcastSet(this.env.fromElements(iteration), "iterations")
                        .collect();

                weights_transitive = this.env.fromCollection(weights_tmp);
                DataSource<double[]> weights_transitive2 = this.env.fromCollection(weights_tmp);

                List<Boolean> next_exits = weights_transitive2
                        .map(new ComputeNormFlink()).withBroadcastSet(weights_set, "weights")
                        .map(norm -> {
                            return (norm.f0 < accuracy * Math.max(norm.f1, 1.0));
                        })
                        .reduce(new ReduceFunction<Boolean>() {
                            @Override
                            public Boolean reduce(Boolean value1, Boolean value2) throws Exception {
                                return value1 && value2;
                            }
                        }).collect();

                if(iteration >= max_iterations){
                    condition = true;
                }else{
                    if(next_exits.size() == 1) {
                        condition = next_exits.get(0);
                    }else{
                        condition = true;
                    }
                }
                iteration++;

            } while (!condition);
        } catch (Exception e) {
            e.printStackTrace();
        }

        weights_set
            .map(
                vector -> {
                    return Arrays
                            .toString(vector)
                            .replace("[", "")
                            .replace("]", "");
                }
            )
            .writeAsText(output);
    }

}

class ComputeNormFlink extends RichMapFunction<double[], Tuple2<Double, Double>>{

    double[] previousWeights;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.previousWeights = getRuntimeContext().<double[]>getBroadcastVariable("weights").get(0);
    }

    @Override
    public Tuple2<Double, Double> map(double[] weights) throws Exception {
        double normDiff = 0.0;
        double normWeights = 0.0;
        for (int j = 0; j < weights.length; j++) {
            normDiff += Math.abs(weights[j] - previousWeights[j]);
            normWeights += Math.abs(weights[j]);
        }
        return new Tuple2<>(normDiff, normWeights);
    }
}


class TransformPerPartitionFlink implements MapPartitionFunction<String, double[]>{

    int features;

    public TransformPerPartitionFlink(int features) {
        this.features = features;
    }

    @Override
    public void mapPartition(Iterable<String> values, Collector<double[]> out) throws Exception {
        values.forEach(line -> {
            String[] pointStr = line.split(" ");
            double[] point = new double[features+1];
            point[0] = Double.parseDouble(pointStr[0]);
            try {
                for (int i = 1; i < pointStr.length; i++) {
                    if (pointStr[i].equals("")) {
                        continue;
                    }
                    String kv[] = pointStr[i].split(":", 2);
                    point[Integer.parseInt(kv[0]) - 1] = Double.parseDouble(kv[1]);
                }
            }catch (Exception e){
                throw new ExperimentException("the error is "+line, e);
            }
            out.collect(point);
        });
    }
}

class ComputeLogisticGradientPerPartitionFlink extends RichMapPartitionFunction<double[], double[]> {
    double[] weights;
    double[] sumGradOfPartition;
    int features;
    boolean first = true;

    public ComputeLogisticGradientPerPartitionFlink(int features) {
        this.features = features;
        sumGradOfPartition = new double[features + 1]; //position 0 is for the count
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.weights = getRuntimeContext().<double[]>getBroadcastVariable("weights").get(0);
        sumGradOfPartition = new double[features + 1];
    }

    @Override
    public void mapPartition(Iterable<double[]> values, Collector<double[]> out) throws Exception {
        values.forEach(point -> {
            double dot = 0;
            for (int j = 0; j < weights.length; j++)
                dot += weights[j] * point[j + 1];
            for (int j = 0; j < weights.length; j++)
                sumGradOfPartition[j + 1] += ((1 / (1 + Math.exp(-1 * dot))) - point[0]) * point[j + 1];

            sumGradOfPartition[0] += 1; //counter for the step size required in the update
        });
        out.collect(sumGradOfPartition);
    }
}