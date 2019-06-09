package org.qcri.rheem.spark.debug;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.collect.Iterators;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.qcri.rheem.basic.data.debug.DebugTagType;
import org.qcri.rheem.basic.data.debug.DebugTuple;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.spark.compiler.debug.IteratorOneElement;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SnifferDispacher<Type> implements FlatMapFunction<Type, Type> {
    //private static int BUFFER_SIZE = 800000;
    private static int BUFFER_SIZE = 1;
    private transient int internal_counter = 0;
    private transient int current_buffer = 0;
    private transient URI uri;
    private transient URI uri2;
    private transient HttpRequestFactory requestFactory;
    private transient HttpRequest request;
    private transient ByteArrayOutputStream bos;
    private transient ObjectOutputStream oos;
    private transient boolean isDebugTuple = true;
    private transient boolean isCloneable = true;
    private boolean isFirstExecution = true;
    private List<DebugTagType> validateTags;
    private transient Executor pool;

    private transient Thread sender_thread;
    private transient Thread cloner_thread;
    private transient ByteBuffer buffer;
    private transient byte[] bytes_content;
    private transient ByteBuffer bytes_buffer;
    private transient int index_content;
    //private transient
    public SnifferDispacher(){
        this.validateTags = new ArrayList<>();
        this.validateTags.add(DebugTagType.MONITOR);
        //this.validateTags.add(DebugTagType.MONITOR);
    }

    @Override
    public Iterator<Type> call(Type type) throws Exception {
        if(this.isFirstExecution){
            this.isFirstExecution = false;
            this.isDebugTuple = type.getClass() == DebugTuple.class;
            this.isCloneable = type instanceof Cloneable;
          //  try {
                uri = URI.create("http://10.4.4.49:8080/debug/add");
                uri2 = URI.create("http://10.4.4.49:8080/debug/store");
                this.requestFactory = new NetHttpTransport().createRequestFactory();
                this.request = requestFactory.buildPostRequest(new GenericUrl(uri), null);
                this.bos = new ByteArrayOutputStream();
                this.oos = new ObjectOutputStream(bos);
                this.pool = new ThreadPoolExecutor(1, 1,
                      25L, TimeUnit.MILLISECONDS,
                      new LinkedBlockingQueue<Runnable>());
                ((ThreadPoolExecutor)pool).setKeepAliveTime(25, TimeUnit.MILLISECONDS);
              //  this.pool = Executors.newWorkStealingPool(10);
           /* } catch (IOException e) {
                e.printStackTrace();
            }*/
           //this.buffer = new Object[BUFFER_SIZE];
          // this.bytes_content = new byte[BUFFER_SIZE*100];
            this.bytes_buffer = ByteBuffer.wrap(new byte[BUFFER_SIZE*200]);
          // this.buffer = new byte[BUFFER_SIZE][];
           this.index_content = 0;
        }
        final DebugTuple<Type> tuple;
        if(this.isDebugTuple) {
            tuple = (DebugTuple<Type>) type;
            /*this.validateTags.forEach(
                tagType -> {
                    if(tuple.getTag(tagType) != null){

                    }
                }
            );*/
           // tuple.cleanTag();
        }else{
            tuple = null;
        }
        StringBuilder myContent = new StringBuilder();
            if(this.isDebugTuple) {
                if(tuple.isDebug()) {
                  //  byte[] byte_value = tuple.getByte();
                    //this.bytes_buffer.putLong(Long.MAX_VALUE);
                  //  this.bytes_buffer.putLong(0L);
                   // this.bytes_buffer.putLong(Long.MAX_VALUE);
                    //this.bytes_buffer.putLong(System.currentTimeMillis());

                  //  this.bytes_buffer.put(byte_value);
                 //   if(tuple.getValue().getClass() == String.class) {
                 //       this.bytes_buffer.put(((String) tuple.getValue()).getBytes());
                //    }
                    internal_counter++;
                }
            }
            if(internal_counter >= BUFFER_SIZE) {
            //    final Object[] old_buffer = this.buffer;
            //    this.buffer = new Object[BUFFER_SIZE];
                   // this.bytes_buffer.hasArray();
                   // final byte[] lala = this.bytes_buffer.array();
                   // for(int i = 0; i < lala.length; i++)
                   //     lala[i] = (byte)256;
                   // printArrayByte(lala, "buffer before");
                    //this.bytes_buffer = ByteBuffer.wrap(new byte[BUFFER_SIZE*200]);
                   // if (((ThreadPoolExecutor) pool).getActiveCount() == 0) {
                   //     this.pool.execute(() -> {
                   //         try {
                                myContent.append(System.currentTimeMillis());
                                HttpContent content = ByteArrayContent.fromString(null, myContent.toString());
                                HttpRequest current_request = requestFactory.buildPostRequest(new GenericUrl(uri), content);
                HttpResponse answer = current_request
                        //.setNumberOfRetries(0)
                        // .setConnectTimeout(1)
                        //.setReadTimeout(0)
                        .execute();
                long current_answer = System.currentTimeMillis();


                /*ByteArrayOutputStream aux_bos = new ByteArrayOutputStream();
                ObjectOutputStream aux_oos = new ObjectOutputStream(aux_bos);
                answer.download(aux_bos);*/
                myContent.append(" ");
                try (BufferedReader br = new BufferedReader(new InputStreamReader(answer.getContent()))) {
                    myContent.append(br.lines().collect(Collectors.joining(System.lineSeparator())));
                }
                myContent.append(" ");
                myContent.append(current_answer);
                answer.disconnect();



                /*myContent.append()
                ByteBuffer tmp_buffer = ByteBuffer.wrap(new byte[aux_bos.size() + 20]);
                tmp_buffer.putLong(current_answer);
                tmp_buffer.put(aux_bos.toByteArray());*/
                HttpContent content2 = ByteArrayContent.fromString(null, myContent.toString());

                requestFactory.buildPostRequest(new GenericUrl(uri2), content2).execute();
                //System.out.println(myContent);


                    /*        } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });*/
                   // }

                bytes_content = null;

                this.internal_counter = 0;
                this.index_content = 0;
            }


        if(this.isDebugTuple || tuple != null){
            tuple.cleanTag();
        }
        return new IteratorOneElement<>(type);
    }


}

