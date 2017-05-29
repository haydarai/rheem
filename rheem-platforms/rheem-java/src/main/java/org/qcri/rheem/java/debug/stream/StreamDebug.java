package org.qcri.rheem.java.debug.stream;

import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.JSONSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by bertty on 16-05-17.
 */
public class StreamDebug {

    private static Map<String, StreamRheem> streams = new HashMap<>();


    public static Stream getStream(Operator op){
        if( ! streams.containsKey(op.getName()) ){
            streams.put(op.getName(), getStreamRheem(op));
        }
        StreamRheem iter = streams.get(op.getName());
        return getStream(iter);
    }

    private static StreamRheem getStreamRheem(Operator op){
        if( op instanceof CollectionSource ){
            return new CollectionStream((CollectionSource) op);
        }

        if( op instanceof TextFileSource ){
            return new FileStream((TextFileSource) op);
        }

        if( op instanceof JSONSource ){
            return new FileStream((JSONSource) op);
        }

        return null;
    }

    public static Stream getStream(Collection collection){
        StreamRheem iter = new CollectionStream(collection);
        return getStream(iter);
    }

    public static Stream getStream(String path){
        StreamRheem iter = new ObjectFileStream(path);
        return getStream(iter);
    }

    private static Stream getStream(StreamRheem iterator){
        if(iterator != null){
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                    iterator, Spliterator.ORDERED | Spliterator.NONNULL), false);
        }
        return Stream.empty();
    }
}
