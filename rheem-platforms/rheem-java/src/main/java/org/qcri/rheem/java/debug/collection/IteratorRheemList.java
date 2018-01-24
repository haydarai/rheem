package org.qcri.rheem.java.debug.collection;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Created by bertty on 02-07-17.
 */
public class IteratorRheemList<T> implements Iterator<T>{
    private RheemNode<T> current;
    private int index;

    public IteratorRheemList(RheemNode<T> first){
        this.current = first;
        this.index = 0;
    }

    public boolean hasNext() {
        if(this.index < this.current.getOccupied()){
            return true;
        }
        if(this.current.getNext() != null){
            this.current = this.current.getNext();
            this.index = 0;
            return hasNext();
        }
        return false;
    }

    public T next() {
        return this.current.get(index++);
    }

    public void remove() {
        this.current.remove(index);
    }

    public void forEachRemaining(Consumer<? super T> action) {

    }
}
