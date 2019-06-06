package org.qcri.rheem.functions;

//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.io.Output;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 1, jvmArgs = {"-Xms10G", "-Xmx10G"})
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
    private String word;
    private String[] words;
    private DebugTuple<String>[] tuples;
    private ByteArrayOutputStream bos;
    private ObjectOutputStream oos;
    /*private Kryo kryo;
    private Output saver;*/

    @Param({"1000"})
    private int SIZE;

    @Setup
    public void setup() {
        this.base = RheemUUID.randomUUID().createChild().createChild();
        this.key = new RheemUUIDKey().createChild().createChild();
        this.word = "QfDXCGrCDl" +
        //          "OqHcWtsZ4r" +
        //        "3NvVagxR5niDWwZ9EXHm" +
        //        "lzwURZ5pII1cji1Dc5CP" +
        //        "OIAFlHdKPqH3Op042tM6" +
        //        "KipX7OTnHt5tMyzpD8TI" +
                  ""
        ;

        this.words = new String[this.SIZE];
        this.tuples = new DebugTuple[this.SIZE];
        Random random = new Random();
        for(int i =0 ; i < this.words.length; i++){
            char[] word = new char[random.nextInt(8)+3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for(int j = 0; j < word.length; j++)
            {
                word[j] = (char)('a' + random.nextInt(26));
            }
            this.words[i] = new String(word);
            this.tuples[i] = new DebugTuple<String>(this.key.createChild(), this.words[i], String.class);
        }

        try {
            this.bos = new ByteArrayOutputStream();
            this.oos = new ObjectOutputStream(bos);
         /*   this.kryo = new Kryo();
            this.kryo.register(DebugTuple.class);
            this.kryo.register(DebugTuple[].class);
            this.kryo.register(RheemUUID.class);
            this.kryo.register(RheemUUIDKey.class);
            this.kryo.register(DebugKey.class);
            this.kryo.register(int[].class);
            this.kryo.register(Class.class);
            this.saver = new Output(this.bos);*/
        } catch (IOException e) {
            e.printStackTrace();
        }
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
         //   blackhole.consume(new DebugTuple(key.createChild(), null));
        }
        blackhole.consume(tuple);
    }

   // @Benchmark
    public void rheemTupleNullKeyCreation(Blackhole blackhole){
       /* DebugTuple tuple = new DebugTuple((DebugKey) null, null);
        blackhole.consume(tuple);*/
    }

    /*@Benchmark
    public void rheemTupleNullHeaderCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple((DebugHeader) null, null);
        blackhole.consume(tuple);
    }*/

   // @Benchmark
    public void rheemTupleNullObjectCreation(Blackhole blackhole){
        DebugTuple tuple = new DebugTuple(null);
        blackhole.consume(tuple);
    }

   // @Benchmark
    public void rheemTupleHeaderConstantCreation(Blackhole blackhole){
        /*DebugTuple tuple = new DebugTuple( this.key, null);
        blackhole.consume(tuple);*/
    }


  //  @Benchmark
    public void rheemTupleHeaderChildCreation(Blackhole blackhole){
       /* DebugTuple tuple = new DebugTuple( this.key.createChild(), null);
        blackhole.consume(tuple);*/
    }

 //   @Benchmark
    public void rheemUUIDCreation(Blackhole blackhole){
        RheemUUID uuid = RheemUUID.randomUUID();
        blackhole.consume(uuid);
    }

   // @Benchmark
    public void rheemUUIDCreationChild(Blackhole blackhole){
        RheemUUID uuid = this.base.createChild();
        blackhole.consume(uuid);
    }

    //@Benchmark
    public void rheemUUIDtoBytes(){
        RheemUUID key = this.base;
        key.tobyte();
        key.bytes = null;
    }

    //@Benchmark
    public void rheemUUIDtoString2(){
        RheemUUID key = this.base;
        Arrays.toString(key.tobyte());
        key.bytes = null;
    }

    //@Benchmark
    public void rheemUUIDtoString(Blackhole hole){
        RheemUUID key = this.base;
        key.toString();
    }
   // @Benchmark
    public void rheemUUIDtoBytesNew(){
        RheemUUID key = this.base;

        key.bytes = null;
    }

   // @Benchmark
    public void string2Byte(Blackhole hole){
        String tmp = this.word;
        hole.consume(tmp.getBytes());
    }

   // @Benchmark
    public void string2Byte2(Blackhole hole){
        char[] vec = this.word.toCharArray();
        byte[] result = new byte[vec.length *2];
        for(int i = 0, index = 0; i < vec.length; i++, index += 2){
            result[index + 1] = (byte) (vec[i] >>= 8);
            result[index] = (byte) (vec[i]);
        }
        hole.consume(result);
        hole.consume(vec);
    }

   // @Benchmark
    public void string2Byte3(Blackhole hole){
        char[] buffer = this.word.toCharArray();
        byte[] b = new byte[buffer.length << 1];
        for(int i = 0; i < buffer.length; i++) {
            int bpos = i << 1;
            b[bpos] = (byte) ((buffer[i]&0xFF00)>>8);
            b[bpos + 1] = (byte) (buffer[i]&0x00FF);
        }
        hole.consume(b);
        hole.consume(buffer);
    }


    //@Benchmark
    public void rheemUUIDtoBytesConservingValue(){
        RheemUUID key = this.base;
        key.tobyte();
    }

    //@Benchmark
    public void timeStramp(Blackhole hole){
        System.currentTimeMillis();
    }

    //@Benchmark
    public void serializeOneByOneString(){
        for(int i = 0; i < this.words.length; i++) {
            try {
                this.oos.writeObject(this.words[i]);
                this.oos.flush();
                bos.toByteArray();
                this.bos.reset();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //@Benchmark
    public void serializeBatchString(){
        for(int i = 0; i < this.words.length; i++) {
            try {
                this.oos.writeObject(this.words[i]);
                if(i % 1000 == 0) {
                    this.oos.flush();
                    bos.toByteArray();
                    this.bos.reset();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //@Benchmark
    public void serializeVectorString(){
        try {
            this.oos.writeObject(this.words);
            this.oos.flush();
            bos.toByteArray();
            this.bos.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void serializeOneByOneTuple(){
        for(int i = 0; i < this.tuples.length; i++) {
            try {
                this.oos.writeObject(this.tuples[i]);
                this.oos.flush();
                bos.toByteArray();
                this.bos.reset();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    public void serializeBatchTuple(){
        for(int i = 0; i < this.tuples.length; i++) {
            try {
                this.oos.writeObject(this.tuples[i]);
                if(i % 1000 == 0) {
                    this.oos.flush();
                    bos.toByteArray();
                    this.bos.reset();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Benchmark
    public void serializeVectorTuple(){
        try {
            this.oos.writeObject(this.tuples);
            this.oos.flush();
            bos.toByteArray();
            this.bos.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
/*
    @Benchmark
    public void serializeKryoOneByOneTuple(){
        for(int i = 0; i < this.tuples.length; i++) {
            this.kryo.writeObject(this.saver, this.tuples[i]);
            this.saver.toBytes();
            this.saver.reset();
            this.bos.reset();
        }
    }

    @Benchmark
    public void serializeKryoBatchTuple(){
        for(int i = 0; i < this.tuples.length; i++) {
            this.kryo.writeObject(this.saver, this.tuples[i]);
            if(i % 1000 == 0) {
                this.saver.toBytes();
                this.saver.reset();
                this.bos.reset();
            }
        }
    }

    @Benchmark
    public void serializeKryoVectorTuple(){
        //for(int i =1; i < 9; i++) {
            this.kryo.writeObject(this.saver, this.tuples);
            this.saver.flush();
            this.saver.toBytes();
            this.saver.reset();
            this.bos.reset();
     /*   }
        this.kryo.writeObject(this.saver, Arrays.copyOfRange(this.tuples, 9001, 9999));
        this.saver.toBytes();
        this.saver.reset();* /
    } */
}
