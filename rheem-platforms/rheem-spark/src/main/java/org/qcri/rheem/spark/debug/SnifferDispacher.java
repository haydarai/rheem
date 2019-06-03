package org.qcri.rheem.spark.debug;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.qcri.rheem.core.function.FunctionDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;

public class SnifferDispacher<Type> implements FunctionDescriptor.SerializablePredicate<Type>{
    private transient int internal_counter = 0;
    private transient URI uri;
    private transient HttpRequestFactory requestFactory;
    private transient HttpRequest request;
    private transient ByteArrayOutputStream bos;
    private transient ObjectOutputStream oos;

    public SnifferDispacher(){
        try {
            uri = URI.create("http://10.4.4.49:8080/debug/add");
            this.requestFactory = new NetHttpTransport().createRequestFactory();
            this.request = requestFactory.buildPostRequest(new GenericUrl(uri), null);
            this.bos = new ByteArrayOutputStream();
            this.oos = new ObjectOutputStream(bos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean test(Type type) {
        try {
            if(internal_counter % 1000 == 0) {
                this.oos.writeObject(type);
                this.oos.flush();
                HttpContent content = new ByteArrayContent(null, bos.toByteArray());
                request.setContent(content).executeAsync();
                this.bos.reset();
            }
        } catch (Exception e) { }
          return true;
    }
}

