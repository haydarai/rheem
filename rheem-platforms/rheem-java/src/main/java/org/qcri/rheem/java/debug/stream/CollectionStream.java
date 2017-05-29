package org.qcri.rheem.java.debug.stream;

import org.qcri.rheem.core.debug.ModeRun;
import org.qcri.rheem.basic.operators.CollectionSource;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by bertty on 16-05-17.
 */
public class CollectionStream<T> extends StreamRheem {

    private Collection<T> collection;

    private Iterator<T> iterator;

    public CollectionStream(CollectionSource<T> op) {
        super();
        this.collection = op.getCollection();
        this.iterator   = this.collection.iterator();
    }

    public CollectionStream(Collection<T> collection) {
        super();
        this.collection = collection;
        this.iterator   = this.collection.iterator();
    }

    @Override
    public boolean hasNext() {
        if( ModeRun.isStopProcess() ){
            return false;
        }
        /*
        for continues processing
        if( ! iterator.hasNext() ){
            this.iterator = collection.iterator();
        }*/
        return iterator.hasNext();
    }

    @Override
    public Object next() {
        return iterator.next();
    }

    @Override
    protected long getSize() {
        return 0;
    }

    @Override
    protected long getCurrent() {
        return 0;
    }
}
