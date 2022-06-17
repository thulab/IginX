package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.driver.PemjaWorker;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;

public class PemjaWriter implements Writer {

    private final PemjaWorker worker;

    public PemjaWriter(PemjaWorker worker) {
        this.worker = worker;
    }

    @Override
    public void writeBatch(BatchData batchData) throws WriteBatchException {
        worker.process(batchData);
    }
}
