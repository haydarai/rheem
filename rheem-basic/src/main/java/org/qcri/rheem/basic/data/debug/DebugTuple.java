package org.qcri.rheem.basic.data.debug;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.debug.key.RheemUUIDKey;

import java.io.Serializable;


public class DebugTuple<Type> implements Serializable {
    private DebugKey key;
    private Type content;
    private Class<Type> type;

    public DebugTuple(){ }

    public DebugTuple(DebugKey key_default, Type content, Class<Type> clazz){
        this.key = key_default;
        this.content = content;
        this.type = clazz;
    }

    public DebugTuple(DebugKey key_origin, Type content){
        this(key_origin, content, (Class<Type>) content.getClass());
    }

    public DebugTuple(Type content){
        this(GENERATOR.build(), content);
    }

    public DebugTuple(Type content, Class<Type> typeClass){
        this(GENERATOR.build(), content, typeClass);
    }

    public DebugKey getHeader(){
        return this.key;
    }

    public Type getValue(){
        return this.content;
    }

    public DebugTuple<Type> setValue(Type new_value){
        this.content = new_value;
        return this;
    }

    public <NewType> DebugTuple<NewType> setValue(NewType new_value, Class<NewType> clazz){
        DebugTuple<NewType> tuple_new = new DebugTuple<>(this.key, new_value);
        this.content = null;
        this.key = null;
        this.type = null;
        return tuple_new;
    }

    public Class<Type> getType(){
        return this.type;
    }

    @Override
    public String toString() {
        return "DebugTuple{" +
                "header=" + this.getHeader() +
                ", value=" + this.getValue() +
                '}';
    }

    //TODO complete the generator
    private static DebugKey GENERATOR;

    public static void setGenerator(Class<? extends DebugKey> clazz){
        try {
            GENERATOR = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            GENERATOR = new RheemUUIDKey();
        }
    }
    public static DebugKey buildKey(){
        return GENERATOR.build(true);
    }
    static {
        setGenerator(RheemUUIDKey.class);
    }
}
