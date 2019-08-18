package org.qcri.rheem.spark.compiler.debug;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.debug.DebugKey;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.util.RheemUUID;
import org.qcri.rheem.spark.compiler.MetaFunctionCompiler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebugKeyExtractorAdapter<T, K> implements MetaFunctionCompiler.KeyExtractor<T, K> {

    private final FunctionDescriptor.SerializableFunction<T, K> impl;

    private boolean isFirstRun = true;
    private boolean isDebugTuple = false;

  /*  private transient URI uri;
    private transient HttpRequestFactory requestFactory;
    private transient HttpRequest request;
    private transient SenderThread<K> sender;*/
    private Class<K> keyClass;

    public DebugKeyExtractorAdapter(FunctionDescriptor.SerializableFunction<T, K> impl, Class<K> keyClass) {
        this.impl = impl;
        this.keyClass = keyClass;
    }

    @Override
    public scala.Tuple2<K, T> call(T t) throws Exception {
        if(this.isFirstRun){
            this.isDebugTuple = t.getClass() == DebugTuple.class;
            this.isFirstRun = false;
           /* try {
                uri = URI.create("http://10.4.4.49:8080/debug/add");
                this.requestFactory = new NetHttpTransport().createRequestFactory();

                this.request = requestFactory.buildPostRequest(new GenericUrl(uri), null);

            } catch (IOException e) {
                e.printStackTrace();
            }*/
            //this.sender = new SenderThread<K>(this.request, this.keyClass);
        }
        T value;
        K key;
        if(isDebugTuple){
            DebugTuple<T> tuple = ((DebugTuple<T>)t);
            value = tuple.getValue();
            /*key = this.impl.apply(value);
            this.sender
                .addPair(key, tuple.getHeader())
                .run()
            ;*/
        }else{
            value = t;
        }
        key = this.impl.apply(value);
        return new scala.Tuple2<>(key, value);
    }
/*
    public class SenderThread<K> implements Runnable, Serializable {
        private transient HttpRequest request;
        private transient ByteArrayOutputStream bos;
        private transient ObjectOutputStream oos;


        private static final int SIZE = 1000;
        private int buffer_index = 0;
        private transient ExecutorService pool = Executors.newFixedThreadPool(3);
        private transient Kryo kryo = new Kryo();
        private transient Output saver;
        private boolean isStringSeriazable = false;
        private ArrayList<byte[]> buffer = new ArrayList<>(SIZE);
        private transient HashMap<K, byte[]> serialized;
        private transient StringBuilder content = new StringBuilder();

        public SenderThread(HttpRequest request, Class<K> keyClass) {
            try {
                this.request = request;
                this.bos = new ByteArrayOutputStream(1024*1024);
                this.oos = new ObjectOutputStream(bos);
                if(keyClass == String.class) {
                    this.isStringSeriazable = true;
                }
                this.kryo.register(keyClass);
                this.kryo.register(Tuple2.class);
                this.kryo.register(RheemUUID.class);
                this.saver = new Output(this.bos);
                this.serialized = new HashMap<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public Runnable addPair(K key_value, DebugKey debugKey_value) {
               /* this.oos.writeObject(key_value);
                this.oos.flush();
*/
              /* if(!this.serialized.containsKey(key_value)){
                   this.kryo.writeObject(this.saver, key_value);
                   this.serialized.put(key_value, this.bos.toByteArray());
                   this.bos.reset();
               }
                this.buffer.add(this.serialized.get(key_value));
                this.buffer.add(debugKey_value.getBytes());*/

                /*if(!isStringSeriazable) {
                    this.kryo.writeObject(this.saver, key_value);
                }else{
                    this.bos.write(this.serialize((String)key_value));
                }*/
               // this.bos.write(debugKey_byte);
//                this.kryo.writeObject(this.saver, debugKey_byte);
               // this.bos.reset();
            //              SerializationUtils.serialize(key_byte, this.oos);
   //             this.oos.write(debugKey_value.getBytes());
    //            this.oos.reset();
                /*index++;
                if(index == SIZE){
                    Thread thread = new Thread(this);
                    thread.start();
                }* /
            this.content.append(key_value)
                        .append(debugKey_value.toString())
            ;
            this.buffer_index++;
            return this;
        }


        public byte[] serialize(String line){
            char[] vec = line.toCharArray();
            byte[] result = new byte[vec.length *2];
            for(int i = 0, index = 0; i < vec.length; i++, index += 2){
                result[index + 1] = (byte) (vec[i] >>= 8);
                result[index] = (byte) (vec[i]);
            }
            return result;
        }
        @Override
        public void run() {

            if(buffer_index >= SIZE){
                //try {
                   // this.kryo.writeObject(this.saver, this.buffer);
                   // this.bos.toByteArray();
                    ByteArrayContent.fromString("", content.toString());
                    /*this.request
                            .setContent(
                                    ByteArrayContent.fromString("", content.toString())
                            )
                            .setNumberOfRetries(0)
                            .setConnectTimeout(3)
                            .executeAsync(pool);/
                 /*   this.bos.reset();
                    for(int i = 0; i < buffer.size(); i++){
                        this.bos.write(buffer.get(i));
                    }
                    this.bos.toByteArray();

                    this.bos.reset();
                    /*for(int i = 0; i < this.buffer.length; i++){
                        this.buffer[i] = null;
                    }*/
               /* } catch (IOException e) {
                    e.printStackTrace();
                }* /
                buffer_index = 0;
            }
        }
    }*/
}
