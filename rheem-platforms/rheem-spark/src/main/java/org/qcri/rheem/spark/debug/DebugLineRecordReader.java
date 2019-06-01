package org.qcri.rheem.spark.debug;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;

public class DebugLineRecordReader implements RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(org.apache.hadoop.mapred.LineRecordReader.class.getName());
    private CompressionCodecFactory compressionCodecs;
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    int maxLineLength;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private HttpRequestFactory requestFactory;
    private HttpRequest request;
    private int counter;

    public DebugLineRecordReader(Configuration job, FileSplit split) throws IOException {
        this(job, split, (byte[])null, URI.create("http://10.4.4.32:8080/debug/"));
    }

    public DebugLineRecordReader(Configuration job, FileSplit split, byte[] recordDelimiter, URI uri) throws IOException {
        this.compressionCodecs = null;
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        this.codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        this.fileIn = fs.open(file);
        if (this.isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if (this.codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)this.codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitLineReader(cIn, job, recordDelimiter);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new SplitLineReader(this.codec.createInputStream(this.fileIn, this.decompressor), job, recordDelimiter);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new UncompressedSplitLineReader(this.fileIn, job, recordDelimiter, split.getLength());
            this.filePosition = this.fileIn;
        }

        if (this.start != 0L) {
            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
        }

        this.pos = this.start;

        this.requestFactory = new NetHttpTransport().createRequestFactory();
        this.request = requestFactory.buildGetRequest(new GenericUrl(uri));
    }

    public DebugLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) {
        this(in, offset, endOffset, maxLineLength, (byte[])null);
    }

    public DebugLineRecordReader(InputStream in, long offset, long endOffset, int maxLineLength, byte[] recordDelimiter) {
        this.compressionCodecs = null;
        this.maxLineLength = maxLineLength;
        this.in = new SplitLineReader(in, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
        try {
            URI uri = URI.create("http://10.4.4.32:8080/debug/");
            this.requestFactory = new NetHttpTransport().createRequestFactory();
            this.request = requestFactory.buildGetRequest(new GenericUrl(uri));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public  DebugLineRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException {
        this(in, offset, endOffset, job, (byte[])null);
    }

    public DebugLineRecordReader(InputStream in, long offset, long endOffset, Configuration job, byte[] recordDelimiter) throws IOException {
        this.compressionCodecs = null;
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.in = new SplitLineReader(in, job, recordDelimiter);
        this.start = offset;
        this.pos = offset;
        this.end = endOffset;
        this.filePosition = null;
        URI uri = URI.create("http://10.4.4.32:8080/debug/");
        this.requestFactory = new NetHttpTransport().createRequestFactory();
        this.request = requestFactory.buildGetRequest(new GenericUrl(uri));
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    private boolean isCompressedInput() {
        return this.codec != null;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput() ? 2147483647 : (int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (this.isCompressedInput() && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    private int skipUtfByteOrderMark(Text value) throws IOException {
        int newMaxLineLength = (int)Math.min(3L + (long)this.maxLineLength, 2147483647L);
        int newSize = this.in.readLine(value, newMaxLineLength, this.maxBytesToConsume(this.pos));
        this.pos += (long)newSize;
        int textLength = value.getLength();
        byte[] textBytes = value.getBytes();
        if (textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                textBytes = value.copyBytes();
                value.set(textBytes, 3, textLength);
            } else {
                value.clear();
            }
        }

        return newSize;
    }

    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        if( !validStatusProcessing() ) return false;
        while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
            key.set(this.pos);
            int newSize;
            if (this.pos == 0L) {
                newSize = this.skipUtfByteOrderMark(value);
            } else {
                newSize = this.in.readLine(value, this.maxLineLength, this.maxBytesToConsume(this.pos));
                this.pos += (long)newSize;
            }

            if (newSize == 0) {
                return false;
            }

            if (newSize < this.maxLineLength) {
                return true;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }
        return false;
    }

    public boolean validStatusProcessing(){
        try {
            counter++;
            if( counter % 100 != 0
            //    || true
            ) {
                return true;
            }

            String status_current = this.request
                        .execute()
                        .parseAsString();

            if(status_current.contains("pause")){
                //resume logic here
                int count = 0;
                do {
                    try {
                        Thread.sleep(1000);
                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    status_current = this.request.execute().parseAsString();
                    if(count % 10== 0){
                        System.out.println(status_current);
                    }
                    count++;
                }while (status_current.contains("pause"));
            }
            if(status_current.contains("stop")){
                //kill the process
                return false;
            }else if(status_current.contains("resume")){
                return true;
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    public synchronized float getProgress() throws IOException {
        return this.start == this.end ? 0.0F : Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
    }

    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    public synchronized void close() throws IOException {
        try {
            if (this.in != null) {
                this.in.close();
            }
        } finally {
            if (this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
            }

        }

    }

    /** @deprecated */
    @Deprecated
    public static class LineReader extends org.apache.hadoop.util.LineReader {
        LineReader(InputStream in) {
            super(in);
        }

        LineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        public LineReader(InputStream in, Configuration conf) throws IOException {
            super(in, conf);
        }

        LineReader(InputStream in, byte[] recordDelimiter) {
            super(in, recordDelimiter);
        }

        LineReader(InputStream in, int bufferSize, byte[] recordDelimiter) {
            super(in, bufferSize, recordDelimiter);
        }

        public LineReader(InputStream in, Configuration conf, byte[] recordDelimiter) throws IOException {
            super(in, conf, recordDelimiter);
        }
    }
}
