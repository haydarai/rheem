package com.haydarai.examples;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.jena.Jena;
import org.qcri.rheem.jena.operators.JenaModelSource;

import java.util.Collection;

public class JenaJavaExample {
    public static void main(String[] args) {
        RheemContext context = new RheemContext(new Configuration())
                .withPlugin(Jena.plugin())
                .withPlugin(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Jena")
                .withUdfJarOf(JenaJavaExample.class);

        Collection<Record> records = planBuilder
                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
                .projectRecords(new String[] {"s"})
                .collect();
        
        for (Record record : records) {
            System.out.println(record);
        }
    }
}
