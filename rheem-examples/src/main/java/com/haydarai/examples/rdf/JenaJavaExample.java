package com.haydarai.examples.rdf;

import org.qcri.rheem.api.*;
import org.qcri.rheem.basic.data.Record;
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

        ProjectRecordsDataQuantaBuilder sAndP = planBuilder
                .readModel(new JenaModelSource(args[0], triples))
                .projectRecords(new String[] {"s"});

        Collection<Record> records = sAndP.collect();
        for (Record record : records) {
            System.out.println(record);
        }

//        ProjectRecordsDataQuantaBuilder sAndO = planBuilder
//                .readModel(new JenaModelSource(args[1], "s", "p", "o"))
//                .projectRecords(new String[] {"s", "o"});
//
//        JoinRecordsDataQuantaBuilder records = sAndP
//                .joinRecords(record -> record.getField(0), sAndO, record -> record.getField(0));
//
//        for (Tuple2<Record, Record> record : records.collect()) {
//            System.out.print(record.getField0().getField(0));
//            System.out.print(record.getField0().getField(1));
//            System.out.print(record.getField1().getField(0));
//            System.out.println(record.getField1().getField(1));
//        }
    }
}
