package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ModelSource extends UnarySource<Record> {

    private final String inputUrl;
    private final List<Tuple3<String, String, String>> triples;
    private final List<String> variables;

    public String getInputUrl() {
        return inputUrl;
    }

    public List<Tuple3<String, String, String>> getTriples() {
        return triples;
    }

    public List<String> getVariables() {
        return variables;
    }

    public ModelSource(String inputUrl, List<String[]> triples) {
        this(inputUrl, createOutputDataSetType(triples), triples);
    }

    public ModelSource(String inputUrl, DataSetType<Record> type, List<String[]> triples) {
        super(type);
        this.inputUrl = inputUrl;
        this.triples = new ArrayList<>();
        for (String[] triple : triples) {
            this.triples.add(new Tuple3<>(triple[0], triple[1], triple[2]));
        }
        this.variables = triples.stream().flatMap(Arrays::stream).distinct().collect(Collectors.toList());
    }

    private static DataSetType<Record> createOutputDataSetType(List<String[]> triples) {
        String[] variableNames = triples.stream().flatMap(Arrays::stream).distinct().toArray(String[]::new);
        return DataSetType.createDefault(new RecordType(variableNames));
    }

    public ModelSource(ModelSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.triples = that.getTriples();
        this.variables = that.getVariables();
    }
}
