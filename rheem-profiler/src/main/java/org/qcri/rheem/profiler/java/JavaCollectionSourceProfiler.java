package org.qcri.rheem.profiler.java;

import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.operators.JavaCollectionSource;
import org.qcri.rheem.profiler.core.api.OperatorProfiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for {@link JavaCollectionSource}s.
 */
public class JavaCollectionSourceProfiler extends JavaSourceProfiler {

    private Collection<Object> sourceCollection;

    public <T extends Object>JavaCollectionSourceProfiler(Supplier<?> dataQuantumGenerator, ArrayList<T> collection, Class<T> out) {
        super(()->new JavaCollectionSource<>(collection, DataSetType.createDefault(out)), dataQuantumGenerator);
        this.operatorGenerator = this::createOperator; // We can only pass the method reference here.
    }

    private JavaCollectionSource createOperator() {
        final Object exampleDataQuantum = this.dataQuantumGenerators.get(0).get();
        return new JavaCollectionSource(this.sourceCollection, DataSetType.createDefault(exampleDataQuantum.getClass()));
    }


    @Override
    public void setUpSourceData(long cardinality) throws Exception {
        // Create the #sourceCollection.
        final Supplier<?> dataQuantumGenerator = this.dataQuantumGenerators.get(0);
        this.sourceCollection = new ArrayList<>((int) cardinality);
        for (int i = 0; i < cardinality; i++) {
            this.sourceCollection.add(dataQuantumGenerator.get());
        }
    }

}
