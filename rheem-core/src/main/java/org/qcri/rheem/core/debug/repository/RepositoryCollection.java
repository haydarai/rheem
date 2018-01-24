package org.qcri.rheem.core.debug.repository;

import java.util.ArrayList;
import java.util.Collection;

public class RepositoryCollection<Type>  extends Repository<Type>{

    private ArrayList<Type> collection;
    private Class<Type> type;

    public RepositoryCollection(ArrayList<Type> collection, Class<Type> type) {
        this.collection = collection;
        this.type = type;
    }

    public Collection<Type> getCollection(){
        return (Collection<Type>) this.collection.clone();
    }
}

