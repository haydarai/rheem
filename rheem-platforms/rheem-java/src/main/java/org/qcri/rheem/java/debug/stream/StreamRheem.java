package org.qcri.rheem.java.debug.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bertty on 17-05-17.
 */
public abstract class StreamRheem implements Iterator {

    private static List<StreamRheem> streams   = new ArrayList<>();
    private static List<Long>        finish_in = new ArrayList<>();
    protected      int               index;
    protected      long              finish;
    protected      long              size;
    protected      boolean           lastElementNecessary;


    public StreamRheem(){
        streams.add(this);
        this.index = streams.indexOf(this);
        this.lastElementNecessary = true;
    }








}
