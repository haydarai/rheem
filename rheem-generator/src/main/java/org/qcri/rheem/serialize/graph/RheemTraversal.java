package org.qcri.rheem.serialize.graph;

import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.serialize.graph.traversal.Direction;
import org.qcri.rheem.serialize.graph.traversal.Down2Top;

import java.util.Iterator;

public class RheemTraversal implements Iterator<Operator>, Iterable<Operator> {

    private RheemPlan plan;

    private Direction direction;

    public RheemTraversal(RheemPlan plan){
        this.plan = plan;
        //TODO do this form the configuration file
        this.restart();
    }


    @Override
    public boolean hasNext() {
        return this.direction.hasNext();
    }

    @Override
    public Operator next() {
        return this.direction.getNext();
    }

    @Override
    public Iterator<Operator> iterator() {
        return this;
    }

    public void restart(){
        this.direction = new Down2Top();
        this.direction.setStartPoint(this.plan.getSinks());
    }
}
