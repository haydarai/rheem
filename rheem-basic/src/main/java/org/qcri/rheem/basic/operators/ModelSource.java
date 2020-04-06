package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple3;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

public class ModelSource extends UnarySource<Record> {

    private final String inputUrl;
    private final Tuple3<String, String, String> triple;

    public String getInputUrl() {
        return inputUrl;
    }

    public Tuple3<String, String, String> getTriple() {
        return triple;
    }

    public ModelSource(String inputUrl, String... triple) {
        this(inputUrl, createOutputDataSetType(triple), triple);
    }

    public ModelSource(String inputUrl, DataSetType<Record> type, String... triple) {
        super(type);
        this.inputUrl = inputUrl;
        this.triple = new Tuple3<>(triple[0], triple[1], triple[2]);
    }

    private static DataSetType<Record> createOutputDataSetType(String[] variableNames) {
        return DataSetType.createDefault(new RecordType(variableNames));
    }

    public ModelSource(ModelSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.triple = that.getTriple();
    }
}
