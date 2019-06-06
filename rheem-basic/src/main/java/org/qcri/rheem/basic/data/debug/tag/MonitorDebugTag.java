package org.qcri.rheem.basic.data.debug.tag;

import org.qcri.rheem.basic.data.debug.DebugTag;
import org.qcri.rheem.basic.data.debug.DebugTagType;

public class MonitorDebugTag extends DebugTag {

    public MonitorDebugTag(String operatorName, String ip_host) {
        super(operatorName, ip_host);
        this.type =  DebugTagType.MONITOR;
    }
}
