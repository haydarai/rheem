package org.qcri.rheem.basic.data.debug;

import java.io.Serializable;

public class DebugTimeMark implements Serializable {

    long time_start;
    long time_end;
    long duration;
    String operatorName;

    public DebugTimeMark(String operatorName) {
        this.operatorName = operatorName;
    }

    public long getTime_start() {
        return time_start;
    }

    public DebugTimeMark setTime_start(long time_start) {
        this.time_start = time_start;
        return this;
    }

    public long getTime_end() {
        return time_end;
    }

    public DebugTimeMark setTime_end(long time_end) {
        this.time_end = time_end;
        return this;
    }

    public long getDuration() {
        return duration;
    }

    public DebugTimeMark setDuration(long duration) {
        this.duration = duration;
        return this;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public DebugTimeMark setOperatorName(String operatorName) {
        this.operatorName = operatorName;
        return this;
    }
}
