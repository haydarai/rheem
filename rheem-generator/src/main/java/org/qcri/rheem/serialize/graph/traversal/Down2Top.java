package org.qcri.rheem.serialize.graph.traversal;

import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

public class Down2Top extends Direction{

    Set<Operator> enrolled;
    Queue<Operator> queue;

    public Down2Top(){
        this.queue = new LinkedList<>();
        this.enrolled = new HashSet<>();
    }


    @Override
    public void setStartPoint(Collection<Operator> startPoint) {
        super.setStartPoint(startPoint);
        this.queue.clear();
        this.queue.addAll(startPoint);
        this.enrolled.clear();
        startPoint.stream().forEach(op -> this.enrolled.add(op));
    }

    @Override
    public void addStartPoint(Operator... operators) {
        super.addStartPoint(operators);
        this.queue.addAll(Arrays.asList(operators));
        Arrays.stream(operators).forEach(op -> this.enrolled.add(op));
    }

    @Override
    public Operator getNext() {
        return this.queue.poll();
    }

    @Override
    public boolean hasNext() {
        generateNext();
        return this.queue.peek() != null;
    }


    public void generateNext(){
        Operator current = this.queue.peek();
        if(current == null) return;
        Arrays.stream(current.getAllInputs())
                .map(input -> input.getOccupant().getOwner())
                .filter(op -> !this.enrolled.contains(op))
                .forEach(op -> {
                    this.queue.add(op);
                    this.enrolled.add(op);
                });
    }
}
