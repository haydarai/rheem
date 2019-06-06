package org.qcri.rheem.core.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A type for tuples. Might be replaced by existing classes for this purpose, such as from the Scala library.
 */
public abstract class Tuple implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    protected static final Map<String, Class<?>> CLASSES = new HashMap<>();

    /**
     * gets the numbers of the fields that have the tuple or arity
     *
     * @return the number of the field that have the tuple or arity
     */
    public abstract int getArity();

    /**
     * valid the index is in the range of the fields of the tuple
     *
     * @param index The position of the field, zero indexed.
     * @return true if the index is valid for processing
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    protected boolean validArity(int index) throws IndexOutOfBoundsException{
        if( index < 0 || index > getArity() ){
            throw new IndexOutOfBoundsException("the index is necesary that have value between 0 and "+getArity());
        }
        return true;
    }

    /**
     * Gets the field at the specified position.
     *
     * @param index The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public abstract <T> T getField(int index);

    /**
     * Gets the field at the specified position, throws NullFieldException if the field is null. Used for comparing key fields.
     *
     * @param index The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     * @throws NullException Thrown, if the field at pos is null.
     */
    public <T> T getFieldNotNull(int index){
        T field = getField(index);
        if (field != null) {
            return field;
        } else {
            //TODO: create a message for the exception
            throw new NullException();
        }
    }

    /**
     * Sets the field at the specified position.
     *
     * @param value The value to be assigned to the field at the specified position.
     * @param index The position of the field, zero indexed.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    public abstract <T> void setField(T value, int index);

    /**
     * Gets the number of field in the tuple (the tuple arity).
     *
     * @return The number of fields in the tuple.
     *
    public abstract int getArity();

    /**
     * Shallow tuple copy.
     * @return A new Tuple with the same fields as this.
     */
    public abstract <T extends Tuple> T copy();

    /**
     * @return a new instance with the fields of this instance swapped
     */
    public abstract <T> T swap();

    // --------------------------------------------------------------------------------------------

    /**
     * Gets the class corresponding to the tuple of the given arity (dimensions). For
     * example, {@code getTupleClass(3)} will return the {@code Tuple3.class}.
     *
     * @param arity The arity of the tuple class to get.
     * @return The tuple class with the given arity.
     */
    @SuppressWarnings("unchecked")
    public static Class<? extends Tuple> getTupleClass(int arity) {
        if ( arity < 0 ) {
            throw new IllegalArgumentException("The tuple arity can not negative");
        }
        String clazz = "Tuple"+arity;
        if( ! CLASSES.containsKey(clazz)){
            try {
                Class tuple_class = Class.forName("org.qcri.rheem.basic.data."+clazz);
                if(Tuple.class.isAssignableFrom(tuple_class)){
                    CLASSES.put(clazz, tuple_class);
                }else{
                    return null;
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            } catch (ClassCastException e){
                e.printStackTrace();
                return null;
            }
        }
        return (Class<? extends Tuple>) CLASSES.get(clazz);
    }

    public static<T extends Tuple> int getArityClass(Class<T> clazz) throws IllegalAccessException, InstantiationException {
        if( ! CLASSES.containsValue(clazz)){
            throw new IllegalArgumentException("The tuple class not exist");
        }
        return clazz.newInstance().getArity();
    }
}
