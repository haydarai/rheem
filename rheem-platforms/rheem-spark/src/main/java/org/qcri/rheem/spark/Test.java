package org.qcri.rheem.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test {

    public static void main(String... args){
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContex = new JavaSparkContext(sparkConf);

        sparkContex.textFile("file:///D:\\rheem-debug-mode\\rheem\\rheem-tests\\src\\test\\resources\\some-lines.txt")
                .flatMapToPair( line -> Arrays.stream(line.split("\\W+")).map(word -> new Tuple2<String, Integer>(word, 1)).iterator())
                .reduceByKey((a, b) -> a + b)
                .collect()
                .forEach(
                    System.out::println
                );


    }
}
