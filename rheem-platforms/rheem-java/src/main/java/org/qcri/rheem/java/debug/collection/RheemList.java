package org.qcri.rheem.java.debug.collection;

import java.util.*;

/**
 * Created by bertty on 22-06-17.
 */
public class RheemList<T> implements List<T>, Cloneable, java.io.Serializable{

    private static final int BLOCK_SIZE     = 1000;
    private static final int ARRAY_MAX_SIZE = Integer.MAX_VALUE - 5;
    private RheemNode<T> first;
    private RheemNode<T> last;
    private long size;
    private int block_size;
    private int block;

    public RheemList(){
        this(RheemList.BLOCK_SIZE);
    }

    public RheemList(int block_size){
        this.block_size = block_size;
        this.first = new RheemNode<T>(this.block_size);
        this.last  = this.first;
        this.size  = 0;
    }

    public int size() {
        if(this.size > ARRAY_MAX_SIZE){
            return -1;
        }
        return (int) this.size;
    }

    public long sizeLong(){
        return this.size;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    public boolean contains(Object o) {
        List tmp = new ArrayList();
        tmp.add(0);
        return containsAll(tmp);
    }

    public Iterator<T> iterator() {
        return new IteratorRheemList<T>(this.first);
    }

    public Object[] toArray() {
        int lenght_array = 0;
        if(this.size > RheemList.ARRAY_MAX_SIZE){
            lenght_array = RheemList.ARRAY_MAX_SIZE;
        }else{
            lenght_array = (int) this.size;
        }
        Object[] array = new Object[lenght_array];
        RheemNode<T> head = this.first;
        int position = 0;
        while(lenght_array > position || head == null){
            for(int i = 0; i < head.getOccupied() || lenght_array <= position; i++){
                array[position++] = head.get(i);
            }
            head = head.getNext();
        }
        return array;
    }

    public <T1> T1[] toArray(T1[] a) {
        return (T1[]) toArray();
    }

    public boolean add(T t) {
        if( this.last.isFull()){
            this.last.setNext(new RheemNode<T>());
            this.last = this.last.getNext();
            this.block++;
        }
        this.last.add(t);
        return true;
    }

    public boolean remove(Object o) {
        Iterator<T> elements = this.iterator();
        while(elements.hasNext()){
            if( o.equals(elements.next()) ){
                elements.remove();
                return true;
            }
        }
        return false;
    }

    public boolean containsAll(Collection<?> c) {
        Iterator<T> elements = this.iterator();
        int original_size = c.size();
        if(original_size == 0) return false;
        int finder = 0;

        Collection<Object> collection_copy = new ArrayList<Object>();
        Iterator tmp = c.iterator();
        while(tmp.hasNext()){
            collection_copy.add(tmp.next());
        }
        Iterator iterator_collection;
        while(elements.hasNext()){
            T element = elements.next();
            iterator_collection = collection_copy.iterator();
            if(c.size() > 0) {
                while (iterator_collection.hasNext()) {
                    if (element.equals(iterator_collection.next())) {
                        finder++;
                        iterator_collection.remove();
                        break;
                    }
                }
            }else{
                break;
            }
        }
        return finder == original_size;

    }

    public boolean addAll(Collection<? extends T> c) {
        Iterator iterator = c.iterator();
        while( iterator.hasNext() ) {
            this.add((T) iterator.next());
        }
        return true;
    }

    public boolean addAll(int index, Collection<? extends T> c) {
        Integer num = new Integer(index);
        RheemNode<T> current = searchIndex(num);
        if(current == null) return false;
        return current.addAll(num, c);
    }

    public boolean removeAll(Collection<?> c) {
        return false;
    }

    public boolean retainAll(Collection<?> c) {
        return false;
    }


    public void sort(Comparator<? super T> c) {

    }

    public void clear() {

    }

    public T get(int index) {
        return null;
    }

    public T set(int index, T element) {
        return null;
    }

    public void add(int index, T element) {

    }

    public T remove(int index) {
        return null;
    }

    public int indexOf(Object o) {
        return 0;
    }

    public int lastIndexOf(Object o) {
        return 0;
    }

    public ListIterator<T> listIterator() {
        return null;
    }

    public ListIterator<T> listIterator(int index) {
        return null;
    }

    public List<T> subList(int fromIndex, int toIndex) {
        return null;
    }


    private RheemNode<T> searchIndex(Integer index){
        RheemNode<T> current = this.first;
        while(current != null){
            if(index < current.getOccupied()){
                return current;
            }
            index = index - current.getOccupied();
            current = current.getNext();
        }
        return current;
    }
}
