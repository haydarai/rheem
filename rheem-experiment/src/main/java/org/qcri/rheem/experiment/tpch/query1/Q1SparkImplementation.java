package org.qcri.rheem.experiment.tpch.query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.experiment.implementations.spark.SparkImplementation;
import org.qcri.rheem.experiment.tpch.entities.Entity;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;
import scala.Tuple10;
import scala.Tuple2;

final public class Q1SparkImplementation extends SparkImplementation {
    public Q1SparkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter)parameters.getParameter("input")).getPath();
        String output = ((FileResult)results.getContainerOfResult("output")).getPath();

        String start_string = ((VariableParameter<String>)parameters.getParameter("date_min")).getVariable();
        int delta = ((VariableParameter<Integer>)parameters.getParameter("delta")).getVariable();

        int start_date = Entity.dateValue(start_string);

        this.sparkContext.textFile(input)
                .map(line -> new Entity(line))
                .filter(entity -> {
                    return entity.parseDate(10) <= (start_date - delta);
                })
                .map(entity -> {
                    Tuple10 tuple = new Tuple10<>(
                            entity.getString(8),
                            entity.getString(9),
                            entity.getDouble(4),
                            entity.getDouble(5),
                            (entity.getDouble(5) * (1 - entity.getDouble(6))),
                            (entity.getDouble(5) * (1 - entity.getDouble(6)) * (1 - entity.getDouble(7))),
                            entity.getDouble(4),
                            entity.getDouble(5),
                            entity.getDouble(6),
                            1
                    );
                    return tuple;
                })
                .keyBy(tuple -> new Tuple2(tuple._1(), tuple._2()))
                .reduceByKey(new Function2<Tuple10, Tuple10, Tuple10>() {
                    @Override
                    public Tuple10 call(Tuple10 v1, Tuple10 v2) throws Exception {
                        return new Tuple10<>(
                            v1._1(),
                            v1._2(),
                            (double)v1._3() + (double)v2._3(),
                            (double)v1._4() + (double)v2._4(),
                            (double)v1._5() + (double)v2._5(),
                            (double)v1._6() + (double)v2._6(),
                            (double)v1._7() + (double)v2._7(),
                            (double)v1._8() + (double)v2._8(),
                            (double)v1._9() + (double)v2._9(),
                            (double)v1._10() + (double)v2._10()
                        );
                    }
                })
                .map(new Function<Tuple2<Tuple2, Tuple10>, Tuple10>() {
                    @Override
                    public Tuple10 call(Tuple2<Tuple2, Tuple10> v1) throws Exception {
                        Tuple10 record = v1._2();
                        return new Tuple10<>(
                                record._1(),
                                record._2(),
                                record._3(),
                                record._4(),
                                record._5(),
                                record._6(),
                                (double)record._7()/(double)record._10(),
                                (double)record._8()/(double)record._10(),
                                (double)record._9()/(double)record._10(),
                                record._10()

                        );
                    }
                })
                .saveAsTextFile(output);
    }
}
