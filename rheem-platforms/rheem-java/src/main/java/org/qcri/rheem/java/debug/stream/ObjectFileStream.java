package org.qcri.rheem.java.debug.stream;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.*;

/**
 * Created by bertty on 22-05-17.
 */
public class ObjectFileStream extends StreamRheem {

    private String         path;
    private BufferedReader bufferedReader;
    private RandomAccessFile randomAccessFile;
    private String         lineCurrent;

    public ObjectFileStream(String name_path) {
        super();
        this.path = name_path;
        this.bufferedReader = openFile();
    }


    private BufferedReader openFile(){
        try {
            FileSystem fs = FileSystems.getFileSystem(this.path).orElseThrow(
                    () -> new RheemException(String.format("Cannot access file system of %s.", this.path))
            );
            fs.open(this.path);
            final InputStream inputStream = fs.open(this.path);
            return new BufferedReader(new InputStreamReader(inputStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void closeFile(){
        try {
            if (this.bufferedReader != null) {
                this.bufferedReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next() {
        return null;
    }

    @Override
    protected long getSize() {
        return 0;
    }

    @Override
    protected long getCurrent() {
        return 0;
    }
}
