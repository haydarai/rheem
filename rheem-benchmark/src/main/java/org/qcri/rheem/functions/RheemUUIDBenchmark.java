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
import org.qcri.rheem.basic.data.debug.DebugHeader;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.basic.data.debug.key.RheemUUIDKey;
import org.qcri.rheem.core.util.RheemUUID;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
@State(Scope.Benchmark)
public class RheemUUIDBenchmark {

    //@Param({"1", "10", "100", "1000", "10000"})
    //@Param({"1", "10", "100"})
    private int N_child;
    private RheemUUID base;
    private DebugKey key;

    @Setup
    public void setup() {
        this.base = RheemUUID.randomUUID();
        this.key = new RheemUUIDKey();
    }

    //@Benchmark
    public void rheemUUIDxChildCreation(Blackhole blackhole){
        for(int i = 0; i < this.N_child; i++){
            blackhole.consume(base.createChild());
        }
    }

   // @Benchmark
    public void rheemTuplexChildCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple(null);
        DebugKey key = tuple.getHeader();
        for(int i = 0; i < this.N_child; i++){
            blackhole.consume(new DebugTuple(key.createChild(), null));
        }
        blackhole.consume(tuple);
    }

    @Benchmark
    public void rheemTupleNullKeyCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple((DebugKey) null, null);
        blackhole.consume(tuple);
    }

    /*@Benchmark
    public void rheemTupleNullHeaderCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple((DebugHeader) null, null);
        blackhole.consume(tuple);
    }*/

    @Benchmark
    public void rheemTupleNullObjectCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple(null);
        blackhole.consume(tuple);
    }

    @Benchmark
    public void rheemTupleHeaderConstantCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple( this.key, null);
        blackhole.consume(tuple);
    }


    @Benchmark
    public void rheemTupleHeaderChildCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple( this.key.createChild(), null);
        blackhole.consume(tuple);
    }

    @Benchmark
    public void rheemUUIDCreation(Blackhole blackhole){
        RheemUUID uuid = RheemUUID.randomUUID();
        blackhole.consume(uuid);
    }

    @Benchmark
    public void rheemUUIDCreationChild(Blackhole blackhole){
        RheemUUID uuid = this.base.createChild();
        blackhole.consume(uuid);
    }

    @Benchmark
    public void rheemUUIDtoBytes(){
        RheemUUID key = this.base;
        key.tobyte();
        key.bytes = null;
    }

    @Benchmark
    public void rheemUUIDtoBytesConservingValue(){
        RheemUUID key = this.base;
        key.tobyte();
    }
}
