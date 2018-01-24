package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.plan.rheemplan.UnarySinkParallel;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Objects;

/**
 * Created by bertty on 14-11-17.
 */
public class SocketSink<T> extends UnarySinkParallel<T> {

    protected final String url;

    protected final TransformationDescriptor<T, String> formattingDescriptor;

    /**
     * Creates a new instance with default formatting.
     *
     * @param url URL to file that should be written
     * @param typeClass   {@link Class} of incoming data quanta
     */
    public SocketSink(String url, Class<T> typeClass) {
        this(
                url,
                new TransformationDescriptor<>(
                        Objects::toString,
                        typeClass,
                        String.class,
                        new NestableLoadProfileEstimator(
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 10 * in[0]),
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 1000)
                        )
                )
        );
    }


    /**
     * Creates a new instance.
     *
     * @param url        URL to file that should be written
     * @param formattingFunction formats incoming data quanta to a {@link String} representation
     * @param typeClass          {@link Class} of incoming data quanta
     */
    public SocketSink(String url,
                        TransformationDescriptor.SerializableFunction<T, String> formattingFunction,
                        Class<T> typeClass) {
        this(
                url,
                new TransformationDescriptor<>(formattingFunction, typeClass, String.class)
        );
    }

    /**
     * Creates a new instance.
     *
     * @param url          URL to file that should be written
     * @param formattingDescriptor formats incoming data quanta to a {@link String} representation
     */
    public SocketSink(String url, TransformationDescriptor<T, String> formattingDescriptor) {
        super(DataSetType.createDefault(formattingDescriptor.getInputType()));
        this.url = url;
        this.formattingDescriptor = formattingDescriptor;
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public SocketSink(SocketSink<T> that) {
        super(that);
        this.url = that.url;
        this.formattingDescriptor = that.formattingDescriptor;
    }

}