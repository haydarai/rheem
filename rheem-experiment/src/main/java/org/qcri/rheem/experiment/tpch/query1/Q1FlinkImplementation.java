package org.qcri.rheem.experiment.tpch.query1;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.util.Collector;
import org.qcri.rheem.experiment.implementations.flink.FlinkImplementation;
import org.qcri.rheem.experiment.tpch.entities.Entity;
import org.qcri.rheem.experiment.utils.parameters.RheemParameters;
import org.qcri.rheem.experiment.utils.parameters.type.FileParameter;
import org.qcri.rheem.experiment.utils.parameters.type.VariableParameter;
import org.qcri.rheem.experiment.utils.results.RheemResults;
import org.qcri.rheem.experiment.utils.results.type.FileResult;
import org.qcri.rheem.experiment.utils.udf.UDFs;

final public class Q1FlinkImplementation extends FlinkImplementation {
    public Q1FlinkImplementation(String platform, RheemParameters parameters, RheemResults result, UDFs udfs) {
        super(platform, parameters, result, udfs);
    }

    @Override
    protected void doExecutePlan() {
        String input = ((FileParameter) parameters.getParameter("lineitem")).getPath();
        String output =((FileResult) results.getContainerOfResult("output")).getPath();

        String start_string = ((VariableParameter<String>)parameters.getParameter("date_min")).getVariable();
        int delta = ((VariableParameter<Integer>)parameters.getParameter("delta")).getVariable();

        int start_date = Entity.dateValue(start_string);


        this.env.readTextFile(input)
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
            .groupBy(0, 1)
            .reduceGroup(new GroupReduceFunction<Tuple10, Tuple10>() {
                @Override
                public void reduce(Iterable<Tuple10> values, Collector<Tuple10> out) throws Exception {
                    double avg_quantity = 0, avg_price = 0, avg_disc = 0;
                    int counter = 0;
                    Tuple10 last = null;
                    for( Tuple10<String, String, Double, Double, Double, Double, Double, Double, Double, Integer> tuple: values){
                        last = tuple;
                        avg_quantity = (double) (avg_quantity + tuple.f6);
                        avg_price = (double) (avg_price + tuple.f7);
                        avg_disc = (double) (avg_disc + tuple.f8);
                        counter = (int) (counter + tuple.f9);
                    }
                    last.setField(avg_quantity/counter, 6);
                    last.setField(avg_price/counter, 7);
                    last.setField(avg_disc/counter, 8);
                    last.setField(counter, 9);
                    out.collect(last);
                }
            }).writeAsText(output);
    }
}
