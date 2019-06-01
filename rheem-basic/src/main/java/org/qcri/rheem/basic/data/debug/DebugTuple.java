package org.qcri.rheem.basic.data.debug;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.data.debug.key.RheemUUIDKey;


public class DebugTuple<Type> extends Tuple2<DebugKey, Object> {


    public DebugTuple(){
        super();
    }

    public DebugTuple(DebugKey parent, Object field1){
        super(parent, field1);
    }

    public DebugTuple(Object field1){
        super(GENERATOR.build(), field1);
    }

    public DebugKey getHeader(){
        return this.getField0();
    }

    public Object getValue(){
        return this.getField1();
    }

    public DebugTuple setValue(Object new_value){
        this.field1 = new_value;
        return this;
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
