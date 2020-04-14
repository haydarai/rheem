package com.haydarai.examples;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.ProjectRecordsDataQuantaBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.jena.Jena;
import org.qcri.rheem.jena.operators.JenaModelSource;

import java.util.Collection;

public class JenaJavaExample {

    public static void main(String[] args) {
        RheemContext context = new RheemContext(new Configuration())
//                .withPlugin(Java.basicPlugin())
                .withPlugin(Jena.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context)
                .withJobName("Jena")
                .withUdfJarOf(JenaJavaExample.class);

        ProjectRecordsDataQuantaBuilder sAndP = planBuilder
                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
                .projectRecords(new String[] {"s", "p"});

        ProjectRecordsDataQuantaBuilder sAndO = planBuilder
                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
                .projectRecords(new String[] {"s", "o"});

        Collection<Tuple2<Record, Record>> records = sAndP
                .join(record -> record.getField(0), sAndO, (record -> record.getField(0)))
                .collect();

//        MapDataQuantaBuilder<Record, Tuple2> sAndP = planBuilder
//                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
//                .projectRecords(new String[] {"s", "p"})
//                .map(record -> new Tuple2<>(record.getField(0), record.getField(1)));
//
//        MapDataQuantaBuilder<Record, Tuple2> sAndO = planBuilder
//                .readModel(new JenaModelSource(args[0], "s", "p", "o"))
//                .projectRecords(new String[] {"s", "o"})
//                .map(record -> new Tuple2<>(record.getField(0), record.getField(1)));
//
//        Collection<Tuple2<Tuple2, Tuple2>> records = sAndP
//                .join((Tuple2::getField0), sAndO, (Tuple2::getField0))
////                .withTargetPlatform(Jena.platform())
//                .collect();
//
//        for (Tuple2<Tuple2, Tuple2> record : records) {
//            System.out.print(record.getField0().getField0());
//            System.out.print(record.getField0().getField1());
//            System.out.print(record.getField1().getField0());
//            System.out.println(record.getField1().getField1());
//        }
    }
}
