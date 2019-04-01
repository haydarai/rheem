package org.qcri.rheem.rest;

import org.qcri.rheem.apps.simwords.SparseVector;
import org.qcri.rheem.basic.data.Tuple2;

import java.util.stream.Stream;

public class TupleSparse extends Tuple2<Integer, SparseVector> {

    public TupleSparse(Integer field0, SparseVector field1) {
        super(field0, field1);
    }

    public TupleSparse add(TupleSparse other){
        return new TupleSparse(this.field0, this.field1.$plus(other.field1));
    }

}
