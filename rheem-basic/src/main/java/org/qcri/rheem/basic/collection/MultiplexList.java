package org.qcri.rheem.basic.collection;


import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MultiplexList<T> extends MultiplexCollection<T> {
    private MultiplexNode<T> head = null;
    private MultiplexNode<T> last = null;
    private Collection<MultiplexIterator> iterators;
    private int size = 0;
    private Thread remover = null;

    {
        this.iterators = new ArrayList<MultiplexIterator>();
    }

    @Override
    protected boolean addElement(T t) {
        size++;
        MultiplexNode<T> node = new MultiplexNode<T>(t);
        if(head == null){
            head = node;
            last = node;
            return true;
        }
        try {
            last.connectTo(node);
            last = node;
            return true;
        } catch (MultiplexException e) {
            e.printStackTrace();
            size--;
            return false;
        }
    }

    @Override
    public Iterator<T> getIterator() {
        generateRemover();
        return generateIterator();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Stream<T> stream() {
        generateRemover();
        return StreamSupport.<T>stream(
                Spliterators.spliteratorUnknownSize(
                        generateIterator(),
                        Spliterator.DISTINCT
                ),
                false);
    }

    private Iterator<T> generateIterator(){
        MultiplexIterator newIterator = new MultiplexIterator<>(this);
        iterators.add(newIterator);
        return newIterator;
    }

    public MultiplexNode<T> getHead() {
        return head;
    }


    private void generateRemover(){
        if(this.remover == null){
            this.remover = new Thread(
                () -> {
                    do{
                        try {
                            Thread.sleep(MultiplexConfiguration.TIME_WAIT);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }while(this.status == MultiplexStatus.WAITING && this.iterators.size() > 0);

                    do {
                        try {
                            Thread.sleep(MultiplexConfiguration.TIME_REVIEW);
                            deleteUntil();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }while(this.status == MultiplexStatus.RUNNING );

                    if(this.status == MultiplexStatus.FINISH_INPUT){
                        //todo: necesito ver si elimino todo o hago otro accion
                    }

                    if(this.status == MultiplexStatus.KILLED){
                        //todo: necesito ver si elimino todo o hago otro accion
                    }
                }
            );
            this.remover.setName("remover");
            this.remover.start();
        }
    }

    private void deleteUntil(){
        System.out.println("moviendo"+head+"   "+ this.status);
        MultiplexNode<T> previous;
        try {
            while(true) {
                /*TODO: descomentar esto es soo por experimentos
                for (MultiplexIterator<T> iterator : iterators) {
                    MultiplexNode<T> tmp = iterator.getPosition();
                    if( ! iterator.isProcessing()){
                        return;
                    }
                    if (iterator.getPosition() == head) {
                        return;
                    }
                }
                */
                previous = head;
                head = head.getNext();
                previous.delete();
                previous = null;
            }
        }catch(Exception e){}
    }

    @Override
    protected void setIterators() {
        for(MultiplexIterator iter: this.iterators){
            iter.setHead(this.head);
        }
    }
}
