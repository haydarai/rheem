package org.qcri.rheem.generator;

import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.flink.platform.FlinkPlatform;
import org.qcri.rheem.generator.plan.Wordcount;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.serialize.RheemIdentifier;
import org.qcri.rheem.serialize.RheemSerializer;
import org.qcri.rheem.serialize.graph.RheemTraversal;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.stream.Collectors;

public class RheemLogGenerator {

    public static void main(String... args) {
        String id_string = args[0];

        String[] id_2_execute;
        if (id_string.contains(",")) {
            id_2_execute = id_string.split(",");
        } else {
            id_2_execute = new String[]{id_string};
        }

        RheemSerializer serializer = new RheemSerializer();
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(Java.basicPlugin());
        rheemContext.register(Spark.basicPlugin());
        rheemContext.register(Flink.basicPlugin());

        for (int i = 0; i < id_2_execute.length; i++) {
            try {
                RheemPlan plan = serializer.recovery(new RheemIdentifier(id_2_execute[i]));
                printPlan(plan);

                rheemContext.execute(
                        plan,
                        ReflectionUtils.getDeclaringJar(Wordcount.class),
                        ReflectionUtils.getDeclaringJar(JavaPlatform.class),
                        ReflectionUtils.getDeclaringJar(SparkPlatform.class),
                        ReflectionUtils.getDeclaringJar(FlinkPlatform.class)
                );
            }catch (Throwable e){
                e.printStackTrace();
            }
        }
    }

    public static void printPlan(RheemPlan plan){
        RheemTraversal traversal = new RheemTraversal(plan);

        for(Operator op: traversal){
            System.out.println(
                String.format(
                    "OP(%s): %s \n\tINPUT: {%s}\n\tOUTPUT: {%s}",
                    op.getTargetPlatforms(),
                    op.toString(),
                    Arrays
                        .stream(op.getAllInputs())
                        .map(input -> input.getOccupant().getOwner().toString())
                        .collect(Collectors.joining(" , ")),
                    Arrays
                        .stream(op.getAllOutputs())
                        .flatMap(output -> output.getOccupiedSlots().stream())
                        .map(input -> input.getOwner().toString())
                        .collect(Collectors.joining(" , "))

                )
            );
        }

    }
}