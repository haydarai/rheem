package org.qcri.rheem.experiment.word2nvec;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateWordNeighborhoodFunction implements FunctionDescriptor.ExtendedSerializableFunction<String, Iterable<Tuple2<Integer, SparseVector>>> {


    private int neighborhoodReach;

    private String dictionaryBroadcastName;

    private Map<String, Integer> dictionary;

    private ArrayList<String> collector = new ArrayList<>();


    public CreateWordNeighborhoodFunction(int neighborhoodReach, String dictionaryBroadcastName) {
        this.neighborhoodReach = neighborhoodReach;
        this.dictionaryBroadcastName = dictionaryBroadcastName;
    }

    @Override
    public void open(ExecutionContext executionContext) {
        this.dictionary = executionContext
                            .<Tuple2<String, Integer>>getBroadcast(dictionaryBroadcastName)
                            .stream()
                            .collect(
                                Collectors.toMap(
                                    ele -> ele.field0,
                                    ele-> ele.field1
                                )
                            );
    }

    @Override
    public Iterable<Tuple2<Integer, SparseVector>> apply(String value) {
        List<Tuple2<Integer, SparseVector>> result = new LinkedList<>();

        this.splitAndScrum(value, this.collector);
/* TODO finish this
        // Make sure that there is at least one neighbor; otherwise, the resulting vector will not support cosine similarity
        if (this.collector.size() > 1) {
            val wordIds = this.collector.map(this.dictionary.getOrElse(_, -1))
            for (i <- wordIds.indices) {
                val builder = new SparseVector.Builder
                for (j <- math.max(0, i - neighborhoodReach) until i; if wordIds(j) != -1) {
                    builder.add(wordIds(j), 1)
                }
                for (j <- i + 1 until math.min(wordIds.size, i + neighborhoodReach + 1); if wordIds(j) != -1) {
                    builder.add(wordIds(j), 1)
                }
                if (!builder.isEmpty) result.add((wordIds(i), builder.build))
            }
            this.collector.clear()
        }
        */
        return result;
    }

    private void splitAndScrum(String string, List<String> collector){
        String[] words = string.split("\\W+");
        for(String word: words){
            if(!word.isEmpty()){
                collector.add(word);
            }
        }
    }
}
