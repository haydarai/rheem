package org.qcri.rheem.basic.collection;


import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class MultiplexCollection<T> extends AbstractCollection<T> {

    protected ArrayList<Iterator> iterators;

    public static int INDEX = 1;
    public String name = "coleccion_"+(INDEX++);

    protected MultiplexStatus status;

    {
        this.iterators = new ArrayList<>();
        this.status  = MultiplexStatus.WAITING;
        MultiplexConfiguration.load(null);
    }


    public boolean add(T t){
        boolean result = addElement(t);
        if(this.status == MultiplexStatus.WAITING){
            setIterators();
            this.status = MultiplexStatus.RUNNING;
        }
        return result;
    }

    protected abstract boolean addElement(T t);

    protected abstract void setIterators();


    public Iterator<T> iterator() {
        Iterator<T> iterator = getIterator();
        this.iterators.add(iterator);
        return iterator;
    }


    protected abstract Iterator<T> getIterator();

    public void setStatus(MultiplexStatus newStatus){
        this.status = newStatus;
    }

    public String toString(){
        return name+" Status: "+this.status;
    }

    public MultiplexStatus getStatus() {
        return status;
    }
}
