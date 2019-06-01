package org.qcri.rheem.basic.data.debug;


import org.qcri.rheem.core.api.exception.RheemException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class DebugKey<KeyType> implements Serializable {
    protected KeyType value;
    protected transient List<KeyType> parent = null;

    public DebugKey(){
        this(true);
    }

    public DebugKey(boolean withdefault){
        this.value = (withdefault)? generateValue() : null;
    }

    public DebugKey(KeyType value, Object... options){
        this.value = value;
    }

    protected DebugKey<KeyType> setValue(KeyType value){
        this.value = value;
        return this;
    }

    public KeyType getValue(){
        return this.value;
    }

    public boolean isSetted(){
        return this.getValue() != null;
    }

    public DebugKey<KeyType> createValue(){
        if( ! this.isSetted()  ){
            //TODO generate a exception propertly
            throw new RheemException("value was assigned");
        }
        this.value = this.generateValue();
        return this;
    }

    public DebugKey<KeyType> addParent(KeyType parent){
        if(this.parent == null){
            this.parent = new ArrayList<>();
        }
        this.parent.add(parent);
        return this;
    }

    public DebugKey<KeyType> cleanParent(){
        if( this.parent != null){
            this.parent.clear();
        }
        return this;
    }

    public List<KeyType> getParents(){
        return this.parent;
    }

    public DebugKey<KeyType> addParents(List<KeyType> parents){
        if(parents == null){
            return this;
        }
        if(this.parent == null){
            this.parent = new ArrayList<>(parents.size());
        }
        this.parent.addAll(parents);
        return this;
    }

    public boolean hasParent(){
        if(this.parent == null) return false;
        return !this.parent.isEmpty();
    }

    public abstract DebugKey<KeyType> createChild();

    protected abstract KeyType generateValue();

    public DebugKey<KeyType> build(){
        return this.build(true);
    }

    public abstract DebugKey<KeyType> build(boolean withDefault);

    public abstract DebugKey<KeyType> plus(DebugKey<KeyType> other);

    public abstract byte[] getBytes();




    @Override
    public String toString() {
        return "DebugKey{" +
                "value=" + value +
                ", parent=" + parent +
                '}';
    }
}
