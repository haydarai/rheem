package org.qcri.rheem.rest;

import org.qcri.rheem.basic.data.Tuple2;

public class TupleSparse extends Tuple2<Integer, Object> {

    public TupleSparse(Integer field0, Object field1) {
        super(field0, field1);
    }

    public TupleSparse add(Object other){
        return new TupleSparse(this.field0, null);
    }

}
