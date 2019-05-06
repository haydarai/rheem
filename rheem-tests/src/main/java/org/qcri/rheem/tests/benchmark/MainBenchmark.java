package org.qcri.rheem.tests.benchmark;

public class MainBenchmark {


    public static void main(String ... args){
        Benchmark base;
//        base = new BaseBenchmark();
        base = new SinkEmptyBenchmark();
        base.execute();


    }
}
