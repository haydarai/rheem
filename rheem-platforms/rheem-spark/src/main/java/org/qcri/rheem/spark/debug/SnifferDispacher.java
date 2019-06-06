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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

public class SnifferDispacher<Type> implements FlatMapFunction<Type, Type> {
    private static int BUFFER_SIZE = 800000;
    private transient int internal_counter = 0;
    private transient int current_buffer = 0;
    private transient URI uri;
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
    private transient Object[] buffer;
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
                this.requestFactory = new NetHttpTransport().createRequestFactory();
                this.request = requestFactory.buildPostRequest(new GenericUrl(uri), null);
                this.bos = new ByteArrayOutputStream();
                this.oos = new ObjectOutputStream(bos);
                this.pool = new ThreadPoolExecutor(1, 1,
                      0L, TimeUnit.MILLISECONDS,
                      new LinkedBlockingQueue<Runnable>());
                ((ThreadPoolExecutor)pool).setKeepAliveTime(5, TimeUnit.MILLISECONDS);
              //  this.pool = Executors.newWorkStealingPool(10);
           /* } catch (IOException e) {
                e.printStackTrace();
            }*/
           //this.buffer = new Object[BUFFER_SIZE];
          // this.bytes_content = new byte[BUFFER_SIZE*100];
            this.bytes_buffer = ByteBuffer.wrap(new byte[BUFFER_SIZE*100]);
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

       /* try { */

//            this.buffer[internal_counter] = type;
           /* if(this.isDebugTuple){
                byte[] byte_value = tuple.getByte();
               /* System.arraycopy(byte_value, 0, bytes_content, index_content, 100);
                index_content += 100;*/
                /*for(int j = 0; j< 100; j++, index_content++){
                    bytes_content[index_content] = byte_value[j];
                }* /
                this.buffer[internal_counter] = byte_value;
            }*/
            if(this.isDebugTuple) {
                byte[] byte_value = tuple.getByte();
                this.bytes_buffer.put(byte_value);
            }
            internal_counter++;
            if(internal_counter >= BUFFER_SIZE) {
            //    final Object[] old_buffer = this.buffer;
            //    this.buffer = new Object[BUFFER_SIZE];
                   // this.bytes_buffer.hasArray();
                    final byte[] lala = this.bytes_buffer.array();
                    this.bytes_buffer = ByteBuffer.wrap(new byte[BUFFER_SIZE*100]);
                    if (((ThreadPoolExecutor) pool).getActiveCount() == 0) {
                        this.pool.execute(() -> {
                            try {
                         /*   final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
                            final ObjectOutputStream objectStream = new ObjectOutputStream(byteBuffer);
                            objectStream.writeObject(old_buffer);
                            byte[] bytes_content = byteBuffer.toByteArray();*/
                                // byte[] bytes_content = new byte[100];
                           /* objectStream.close();
                            byteBuffer.close();*/
                                HttpContent content = new ByteArrayContent(null, lala);
                                HttpRequest current_request = requestFactory.buildPostRequest(new GenericUrl(uri), content);
                                current_request
                                        .setNumberOfRetries(0)
                                        .setConnectTimeout(1)
                                        .setReadTimeout(0)
                                        .execute();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                    }

                bytes_content = null;
                /*    HttpContent content = new ByteArrayContent(null, bytes_content);
                    request
                            .setContent(content)
                            .setNumberOfRetries(0)
                            .setConnectTimeout(1)
                            .setReadTimeout(0)
                            .executeAsync(pool);*/

                this.internal_counter = 0;
                this.index_content = 0;
            }
      /*  } catch (Exception e) {
            System.out.println();
        }*/

        if(this.isDebugTuple || tuple != null){
            tuple.cleanTag();
        }
        return new IteratorOneElement<>(type);
    }
}

