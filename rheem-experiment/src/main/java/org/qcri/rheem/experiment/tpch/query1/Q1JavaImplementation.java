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
import java.util.stream.Collectors;

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

        this.writeFile(
            ((FileResult)results.getContainerOfResult("output")).getURI(),
            this.getStreamOfFile(
                ((FileParameter) parameters.getParameter("lineitem")).getURI()
            ).map(line -> new Entity(line))
            .filter(entity -> {
                return entity.parseDate(10) <= (start_date - delta);
            })
            .map(entity -> {
                final Entity entity1 = new Entity(
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
                return entity1;
            })
            .collect(
                Collectors.toMap(
                    entity -> new Tuple2(entity.getString(0), entity.getString(1)),
                    entity -> entity,
                    (entity1, entity2) -> {
                        final Entity entityfinal = new Entity(
                                entity1.getString(0),
                                entity1.getString(1),
                                entity1.getDouble(2) + entity2.getDouble(2),
                                entity1.getDouble(3) + entity2.getDouble(3),
                                entity1.getDouble(4) + entity2.getDouble(4),
                                entity1.getDouble(5) + entity2.getDouble(5),
                                entity1.getDouble(6) + entity2.getDouble(6),
                                entity1.getDouble(7) + entity2.getDouble(7),
                                entity1.getDouble(8) + entity2.getDouble(8),
                                entity1.getDouble(9) + entity2.getDouble(9),
                                entity1.getDouble(10) + entity2.getDouble(10)
                        );
                        return entityfinal;
                    }
                )
            ).entrySet().stream()
            .map(entry -> {
                Entity original = entry.getValue();
                return new Entity(
                        original.getString(0),
                        original.getString(1),
                        original.getDouble(2),
                        original.getDouble(3),
                        original.getDouble(4),
                        original.getDouble(5),
                        original.getDouble(6),
                        original.getDouble(7) / original.getDouble(10),
                        original.getDouble(8) / original.getDouble(10),
                        original.getDouble(9) / original.getDouble(10),
                        original.getDouble(10)
                ).toString();
            })
        );
    }
}
