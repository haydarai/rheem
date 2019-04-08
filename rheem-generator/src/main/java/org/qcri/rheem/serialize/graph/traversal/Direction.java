package org.qcri.rheem.serialize.graph.traversal;

import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public abstract class Direction {

    protected Collection<Operator> startPoint;

    public void setStartPoint(Collection<Operator> startPoint){
        this.startPoint = startPoint;
    }

    public void addStartPoint(Operator... operators){
        if(this.startPoint == null) {
            this.startPoint = new ArrayList<>(operators.length);
        }
        this.startPoint.addAll(Arrays.asList(operators));
    }

    public abstract Operator getNext();

    public abstract boolean hasNext();
}
