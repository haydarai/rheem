package org.qcri.rheem.spark.debug;

import com.google.common.base.Charsets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.net.URI;

public class DebugTextInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable {
    private CompressionCodecFactory compressionCodecs = null;

    public DebugTextInputFormat() {
    }

    public void configure(JobConf conf) {
        this.compressionCodecs = new CompressionCodecFactory(conf);
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        return null == codec ? true : codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }

        URI uri = URI.create("http://10.4.4.32:8080/debug/");
        return new DebugLineRecordReader(job, (FileSplit)genericSplit, recordDelimiterBytes, uri);
    }
}
