package org.qcri.rheem.apps.wordcount;

import org.python.antlr.op.In;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.Random;

public class Generator {


    public static void main(String ... args){
        String output=args[0];
        int size = Integer.parseInt(args[1]);
        int number = Integer.parseInt(args[2]);
        int tuple = Integer.parseInt(args[3]);
        ArrayList<Integer> collection = new ArrayList<>(size+3);
        for(int i = 0; i < size; i++) {
            collection.add(number);
        }

        CollectionSource<Integer> collectionop = new CollectionSource<Integer>(collection, Integer.class);

        FlatMapOperator<Integer, String> flat = new FlatMapOperator<Integer, String>(
                new FunctionDescriptor.SerializableFunction<Integer, Iterable<String>>() {
                    private final String[] CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split("");

                    Random random = new Random();

                    @Override
                    public Iterable<String> apply(Integer number) {
                        ArrayList<String> lines = new ArrayList<>(number);
                        for (int i = 0; i < number; i++) {
                            lines.add(createRandomString(tuple));
                        }
                        return lines;
                    }

                    private String createRandomString(int len) {
                        StringBuilder sb = new StringBuilder(len);
                        while (sb.length() < len) {
                            sb.append(CHARACTERS[random.nextInt(CHARACTERS.length)]);
                        }
                        return sb.toString();
                    }

                },
                Integer.class,
                String.class
        );

        TextFileSink<String> save = new TextFileSink<String>(output, String.class);

        collectionop.connectTo(0, flat, 0);
        flat.connectTo(0, save, 0);

        RheemPlan plan = new RheemPlan(save);

        RheemContext rheemContext = new RheemContext();
        rheemContext.register(Spark.basicPlugin());


        rheemContext.execute(plan, ReflectionUtils.getDeclaringJar(Generator.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));
    }
}
