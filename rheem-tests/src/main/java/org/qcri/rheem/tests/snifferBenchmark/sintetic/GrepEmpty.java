package org.qcri.rheem.tests.snifferBenchmark.sintetic;

import com.google.api.client.http.HttpRequestFactory;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.TextFileSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.basic.plugin.RheemBasic;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.tests.snifferBenchmark.wordcount.WordCountSpecial;

public class GrepEmpty {
    public static void main(String... args){
        String inputFile = args[0];
        String outputFile = args[1];

        TextFileSource source = new TextFileSource(inputFile);
        FilterOperator<String> filter = new FilterOperator<String>(
                line -> {
                    line.contains("distinguished");
                    return true;
                },
                String.class
        );

        FilterOperator<String> filter_cleanerr = new FilterOperator<String>(
                line -> {
                    return false;
                },
                String.class
        );

        TextFileSink<String> sink = new TextFileSink<String>(
                outputFile,
                record -> record.toString(),
                String.class
        );

        source.connectTo(0, filter, 0);
        filter.connectTo(0, filter_cleanerr, 0);
        filter_cleanerr.connectTo(0, sink, 0);

        execute(new RheemPlan(sink), GrepEmpty.class);
    }

    public static void execute(RheemPlan plan, Class base){
        RheemContext context = new RheemContext();
        context.register(Spark.basicPlugin());

        context.execute(
                plan,
                ReflectionUtils.getDeclaringJar(base),
                ReflectionUtils.getDeclaringJar(SparkPlatform.class),
                ReflectionUtils.getDeclaringJar(HttpRequestFactory.class),
                ReflectionUtils.getDeclaringJar(com.google.api.client.http.HttpContent.class),
                ReflectionUtils.getDeclaringJar(com.google.api.client.http.HttpRequest.class),
                ReflectionUtils.getDeclaringJar(io.opencensus.trace.propagation.TextFormat.class),
                ReflectionUtils.getDeclaringJar(io.opencensus.contrib.http.util.HttpPropagationUtil.class),
                ReflectionUtils.getDeclaringJar(com.google.api.client.http.OpenCensusUtils.class),
                ReflectionUtils.getDeclaringJar(io.grpc.Context.class),
                ReflectionUtils.getDeclaringJar(RheemBasic.class),
                ReflectionUtils.getDeclaringJar(JavaPlatform.class),
                ReflectionUtils.getDeclaringJar(WordCountSpecial.class)
        );
    }
}
