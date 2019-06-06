package org.qcri.rheem.basic.data.debug;

import java.io.Serializable;

public abstract class DebugTag implements Serializable {

    protected long time_start;
    protected long time_end;
    protected long duration;
    protected String operatorName;
    protected String ip_host;
    protected DebugTagType type;


    public DebugTag(String operatorName, String ip_host) {
        this.operatorName = operatorName;
        this.ip_host = ip_host;
    }

    public long getTimeStart() {
        return time_start;
    }

    public DebugTag setTimeStart(){
        this.time_start = System.currentTimeMillis();
        return this;
    }

    public DebugTag setTimeStart(long time_start) {
        this.time_start = time_start;
        return this;
    }

    public long getTimeEnd() {
        return time_end;
    }

    public DebugTag setTimeEnd() {
        this.time_end = System.currentTimeMillis();
        return this;
    }
    public DebugTag setTimeEnd(long time_end) {
        this.time_end = time_end;
        return this;
    }

    public long getDuration() {
        return duration;
    }

    public DebugTag setDuration(long duration) {
        this.duration = duration;
        return this;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public DebugTag setOperatorName(String operatorName) {
        this.operatorName = operatorName;
        return this;
    }

    public DebugTagType getType() {
        return type;
    }
}
