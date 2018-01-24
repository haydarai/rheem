package org.qcri.rheem.basic.collection;


public class MultiplexNode<T> {

    private T element;
    private MultiplexNode<T> next = null;

    public MultiplexNode(T element){
        this.element = element;
        this.next = null;
    }

    public void connectTo(MultiplexNode<T> node) throws MultiplexException {
        if(this.next != null){
            //TODO; create message for this error
            throw new MultiplexException();
        }
        this.next = node;
    }


    public T getElement(){
        return this.element;
    }

    public MultiplexNode<T> getNext(){
        return this.next;
    }

    public void delete(){
        this.element = null;
        this.next = null;
    }



}
