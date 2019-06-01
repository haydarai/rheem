package org.qcri.rheem.collection;

import javolution.util.FastTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 1)
@Measurement(iterations = 2)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
public class CollectionBenchmark {

    @State(Scope.Thread)
    public static class RheemParameters{
        //@Param({"1", "10", "100", "1000", "10000", "100000", "1000000", "10000000", "100000000"})
        @Param({"1", "10", "100", "1000"})
        public int size;
    }

    @Benchmark
    public void collectionArrayListOneByOne(RheemParameters parameters, Blackhole blackhole){
        List<Integer>list = new ArrayList<>();
        for(int i = 0; i < parameters.size; i++){
            list.add(i);
        }
        blackhole.consume(list);
    }

    @Benchmark
    public void collectionArrayListPreSize(RheemParameters parameters, Blackhole blackhole){
        List<Integer>list = new ArrayList<>(parameters.size);
        for(int i = 0; i < parameters.size; i++){
            list.add(i);
        }
        blackhole.consume(list);
    }

    @Benchmark
    public void collectionLinkedListOneByOne(RheemParameters parameters, Blackhole blackhole){
        List<Integer>list = new LinkedList<>();
        for(int i = 0; i < parameters.size; i++){
            list.add(i);
        }
        blackhole.consume(list);
    }

    @Benchmark
    public void collectionFastTableOneByOne(RheemParameters parameters, Blackhole blackhole){
        List<Integer>list = new FastTable<>();
        for(int i = 0; i < parameters.size; i++){
            list.add(i);
        }
        blackhole.consume(list);
    }




}
