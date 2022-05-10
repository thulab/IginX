package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;

public abstract class ExportWriter implements Writer, Exporter {

    private final Mutex mutex = new Mutex();

    public ExportWriter() {

    }

    @Override
    public void writeBatch(BatchData batchData) {
        write(batchData);

        // call the JobRunner to send next batch of data.
        mutex.unlock();
    }

    public abstract void write(BatchData batchData);

    @Override
    public Mutex getMutex() {
        return mutex;
    }
}
