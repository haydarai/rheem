package org.qcri.rheem.giraph.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.channels.DefaultChannelConversion;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.giraph.platform.GiraphPlatform;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.operators.SparkTsvFileSink;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link ChannelConversion}s for the {@link GiraphPlatform}.
 */
public class ChannelConversions {

    public static final ChannelConversion UNCACHED_RDD_TO_HDFS_TSV = new DefaultChannelConversion(
            RddChannel.UNCACHED_DESCRIPTOR,
            FileChannel.HDFS_TSV_DESCRIPTOR,
            () -> new SparkTsvFileSink<>(DataSetType.createDefaultUnchecked(Tuple2.class))
    );
    /**
     * All {@link ChannelConversion}s.
     */
    public static Collection<ChannelConversion> ALL = Arrays.asList(
            UNCACHED_RDD_TO_HDFS_TSV
    );





}
