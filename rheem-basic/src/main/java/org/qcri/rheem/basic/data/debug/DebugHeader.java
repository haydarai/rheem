package org.qcri.rheem.basic.data.debug;

import org.qcri.rheem.basic.data.debug.key.RheemUUIDKey;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DebugHeader implements Serializable {
    //TODO complete the generator
    private static DebugKey GENERATOR;

    private DebugKey id;

    private List<DebugTimeMark> timers = null;

    static {
        setGenerator(RheemUUIDKey.class);
    }

    public DebugHeader(DebugKey key){
        this.id = key;
    }

    public DebugHeader() {
        /*if(DebugHeader.GENERATOR == null){
            DebugHeader.GENERATOR = new RheemUUIDKey();
        }*/
        this.id = DebugHeader.GENERATOR.build();
        //TODO search for a option more fast in terms of list
    }

    public DebugKey getId() {
        return this.id;
    }

    public List<DebugTimeMark> getTimers() {
        if(this.timers == null){
            this.timers = new ArrayList<>();
        }
        return timers;
    }

    public DebugHeader addTimer(DebugTimeMark timeMark){
        if(this.timers == null){
            this.timers = new ArrayList<>();
        }
        this.timers.add(timeMark);
        return this;
    }

    @Override
    public String toString() {
        return "DebugHeader{" +
                "id=" + this.id +
                (
                    (this.timers != null)?
                        ", timers=" + Arrays.toString(this.timers.toArray()):
                        ""
                ) +
                '}';
    }

    public static void setGenerator(Class<? extends DebugKey> clazz){
        try {
            DebugHeader.GENERATOR = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            DebugHeader.GENERATOR = new RheemUUIDKey();
        }
    }

}
