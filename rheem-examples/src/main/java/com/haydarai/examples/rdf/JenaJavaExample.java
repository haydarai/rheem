package com.haydarai.examples.rdf;

import org.qcri.rheem.api.*;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.jena.Jena;
import org.qcri.rheem.jena.operators.JenaModelSource;
import org.qcri.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JenaJavaExample {

    public static void main(String[] args) {
        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Jena.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Jena")
                .withUdfJarOf(JenaJavaExample.class);

        List<String[]> triples = new ArrayList<>();
        triples.add(new String[]{"p", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf", "c"});
        triples.add(new String[]{"s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#takesCourse", "c"});
        triples.add(new String[]{"s", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#advisor", "p"});

        // IDEAL IMPLEMENTATION
//        ProjectRecordsDataQuantaBuilder sAndP = planBuilder
//                .readModel(new JenaModelSource(args[0], triples))
//                .projectRecords(new String[] {"s", "p"});
//
//        ProjectRecordsDataQuantaBuilder sAndC = planBuilder
//                .readModel(new JenaModelSource(args[0], triples))
//                .projectRecords(new String[] {"s", "c"});
//
//        JoinDataQuantaBuilder<Record, Record, Object> joinedRecords = sAndC
//                .join(record -> record.getField(0), sAndP, record -> record.getField(0));
//
//        Collection<Tuple2<Record, Record>> records = joinedRecords.collect();
//        for (Tuple2<Record, Record> record : records) {
//            System.out.print(record.getField0().getField(0));
//            System.out.print(record.getField0().getField(1));
//            System.out.print(record.getField1().getField(0));
//            System.out.println(record.getField1().getField(1));
//        }

        // WORKING IMPLEMENTATION
        MapDataQuantaBuilder<Record, Tuple2<String, String>> sAndP = planBuilder
                .readModel(new JenaModelSource(args[0], triples))
                .projectRecords(new String[]{"s", "p"})
                .map(record -> new Tuple2<>(record.getField(0).toString(), record.getField(1).toString()));

        MapDataQuantaBuilder<Record, Tuple2<String, String>> sAndC = planBuilder
                .readModel(new JenaModelSource(args[0], triples))
                .projectRecords(new String[]{"s", "c"})
                .map(record -> new Tuple2<>(record.getField(0).toString(), record.getField(1).toString()));

        JoinDataQuantaBuilder<Tuple2<String, String>, Tuple2<String, String>, String> joinedRecords = sAndP
                .join(Tuple2::getField0, sAndC, Tuple2::getField0);
        Collection<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> records = joinedRecords.collect();
        for (Tuple2<Tuple2<String, String>, Tuple2<String, String>> record : records) {
            System.out.print(record.getField0().getField0() + ", ");
            System.out.print(record.getField0().getField1() + ", ");
            System.out.print(record.getField1().getField0() + ", ");
            System.out.println(record.getField1().getField1());
        }
    }
}
