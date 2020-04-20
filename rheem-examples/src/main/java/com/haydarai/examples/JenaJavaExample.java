package com.haydarai.examples;

import org.qcri.rheem.api.*;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.jena.Jena;
import org.qcri.rheem.jena.operators.JenaModelSource;
import org.qcri.rheem.spark.Spark;

import java.util.Collection;

public class JenaJavaExample {

    public static void main(String[] args) {
        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Jena.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Jena")
                .withUdfJarOf(JenaJavaExample.class);

        ProjectRecordsDataQuantaBuilder sAndP = planBuilder
                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
                .projectRecords(new String[] {"s", "p"});

        ProjectRecordsDataQuantaBuilder sAndO = planBuilder
                .readModel(new JenaModelSource(args[1], "s", "p", "o"))
                .projectRecords(new String[] {"s", "o"});

        JoinRecordsDataQuantaBuilder records = sAndP
                .joinRecords(record -> record.getField(0), sAndO, record -> record.getField(0));

        for (Tuple2<Record, Record> record : records.collect()) {
            System.out.print(record.getField0().getField(0));
            System.out.print(record.getField0().getField(1));
            System.out.print(record.getField1().getField(0));
            System.out.println(record.getField1().getField(1));
        }
    }
}
