package org.qcri.rheem.functions;


import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.util.RheemUUID;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
@State(Scope.Benchmark)
public class UUIDBenchmark {

    @Benchmark
    public void uuidCreation(Blackhole blackhole){
        UUID uuid = UUID.randomUUID();
        blackhole.consume(uuid);
    }

    @Benchmark
    public void rheemUUIDCreation(Blackhole blackhole){
        RheemUUID uuid = RheemUUID.randomUUID();
        blackhole.consume(uuid);
    }


    @Benchmark
    public void debugTupleCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple(null);
        blackhole.consume(tuple);
    }


}
