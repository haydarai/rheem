package org.qcri.rheem.java.debug.collection;

import java.util.Collection;
import java.util.Iterator;

/**
 * Created by bertty on 22-06-17.
 */
class RheemNode<T> {

    private static int DEFAULT_SIZE = 1000;
    private int occupied;
    private int size;
    private T[] data;
    private RheemNode<T> next;


    public RheemNode(){
        this(RheemNode.DEFAULT_SIZE);
    }

    public RheemNode(int size){
        this.size = size;
        this.occupied = 0;
        this.data = (T[]) new Object[this.size];
        this.next = null;
    }

    public boolean isFull(){
        return this.occupied == this.size;
    }

    public boolean add(T element){
        if(isFull()){
            return false;
        }
        this.data[this.occupied++] = element;
        return true;
    }

    public void setNext(RheemNode<T> next){
        this.next = next;
    }

    public RheemNode<T> getNext(){
        return this.next;
    }

    public T get(int index){
        rangeCheck(index);
        return this.data[index];
    }

    public int getSize(){
        return this.size;
    }

    public int getOccupied(){
        return this.occupied;
    }

    private boolean rangeCheck(int index){
        if (index >= size)
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        if (index < 0)
            throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
        return true;
    }

    private String outOfBoundsMsg(int index) {
        return "Index: "+index+", Size: "+size;
    }

    public T remove(int index){
        rangeCheck(index);

        T oldValue = get(index);

        int numMoved = size - index - 1;
        if (numMoved > 0)
            System.arraycopy(data, index+1, data, index, numMoved);
        data[--size] = null; // clear to let GC do its work
        size--;
        return oldValue;
    }

    public boolean addAll(int index, Collection<? extends T> c){
        int size_collection = c.size();
        int size_real = data.length;

        int new_size = size + size_collection - (size_real - size);

        T[] new_data = (T[]) new Object[new_size];

        if(index != 0){
            System.arraycopy(data, 0, new_data, 0,  index -1 );
        }else{
            index = -1;
        }
        System.arraycopy(data, index + 1, new_data, index + size_collection + 1, this.getOccupied() - index );

        Iterator iterator = c.iterator();
        while(iterator.hasNext()){
            if( ! this.add(index++, (T) iterator.next()) ){
                return false;
            }
        }
        return true;
    }

    public boolean add(int index, T o){
        if(this.data[index] != null) return false;

        this.data[index] = o;
        return true;
    }


}
