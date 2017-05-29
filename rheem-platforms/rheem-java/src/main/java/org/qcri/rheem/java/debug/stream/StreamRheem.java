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

    public static void setNewFinish(int index, long finish){
        StreamRheem tmp = streams.get(index);
        tmp.setFinish(finish);
        finish_in.set(index, finish);
}

    public static void setNewFinish(int index){
        setNewFinish(index, streams.get(index).getCurrent());
    }

    protected void setFinish(long finish){
        this.lastElementNecessary = true;
        this.finish = finish;
        if(finish_in.get(this.index) != this.finish){
            finish_in.set(this.index, this.finish);
        }
    }

    protected void setSize(){
        this.finish = getSize();
        finish_in.add(this.finish);
        this.size = this.finish;
    }

    protected abstract long getSize();

    protected abstract long getCurrent();


}
