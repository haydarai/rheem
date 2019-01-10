package org.qcri.rheem.experiment.enviroment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkEnviroment extends EnviromentExecution<JavaSparkContext> {
    @Override
    public JavaSparkContext getEnviroment() {
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("wordcount");
        return new JavaSparkContext(sparkConf);
    }
}
