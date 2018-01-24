package org.qcri.rheem.basic.collection;

import java.util.Iterator;

/**
 * Created by bertty on 13-11-17.
 */
class MultiplexIterator<T> implements Iterator<T> {
    public static int INDEX=1;
    public String name = "iterator_"+(INDEX++);
    MultiplexList<T> list;
    MultiplexNode<T> pointer;
    MultiplexNode<T> internalLast;
    boolean procesando = false;
    T element;


    public MultiplexIterator(MultiplexList<T> list){
        this.list = list;
        this.pointer = this.list.getHead();
    }
    @Override
    public boolean hasNext() {
        try {
            this.procesando = true;
            boolean estado = _hasNext();
            return estado;
        }catch (Exception e){
            return false;
        }
    }


    public boolean _hasNext() {
        System.out.println("here");
        if(this.list.getStatus() == MultiplexStatus.KILLED){
            return false;
        }
        if(this.list.getStatus() == MultiplexStatus.WAITING){
            do{
                try {
                    Thread.sleep(MultiplexConfiguration.TIME_WAIT);
                    System.out.println("hehehejdgashdghjasgdgasdgkasdgasgd\n\n\n");
                    if(pointer == null){
                        pointer = this.list.getHead();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }while(this.list.getStatus() == MultiplexStatus.WAITING);
            procesando = true;
        }
        //nunca existara mas de un estado wait, o mejor dicho no deberia existir
        if(this.list.getStatus() == MultiplexStatus.RUNNING){
            boolean wait = true;
            if(pointer != null) {
                internalLast = pointer;
            }
            do{
                if( pointer != null ){
                    wait = false;
                }else {
                    try {
                        Thread.sleep(MultiplexConfiguration.TIME_ASKED);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if(internalLast.getNext() != null){
                        wait = true;
                        pointer = internalLast.getNext();
                    }
                    if(this.list.getStatus() != MultiplexStatus.RUNNING){
                        break;
                    }
                }
            }while( wait );
            if(wait == false) {
                element = pointer.getElement();
                internalLast = pointer;
                pointer = pointer.getNext();
                this.procesando = true;
                return true;
            }
        }
        if(this.list.getStatus() == MultiplexStatus.FINISH_INPUT){
            /*if(internalLast != null) {
                pointer = internalLast.getNext();
                internalLast = pointer;
            }*/

            if(pointer == null){
                return false;
            }
            element  = pointer.getElement();
            pointer = pointer.getNext();
            //internalLast = pointer;
            return true;
        }
        return false;
    }

    @Override
    public T next() {
        System.out.println(element);
        return element;
    }

    public MultiplexNode<T> getPosition(){
        if(this.pointer != null){
            return pointer;
        }
        if(this.internalLast != null){
            return internalLast;
        }
        return null;
    }

    public boolean isProcessing(){
        return this.procesando;
    }

    public void setHead(MultiplexNode<T> head){
        this.pointer = head;
    }

}
