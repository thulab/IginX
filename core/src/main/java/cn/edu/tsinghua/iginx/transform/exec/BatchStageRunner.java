package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.transform.driver.PemjaDriver;
import cn.edu.tsinghua.iginx.transform.driver.PemjaWorker;
import cn.edu.tsinghua.iginx.transform.exception.CreateWorkerException;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import cn.edu.tsinghua.iginx.transform.pojo.BatchStage;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStageRunner implements Runner {

    private final BatchStage batchStage;

    private final Mutex mutex;

    private Writer writer;

    private PemjaWorker pemjaWorker;

    private final PemjaDriver driver = PemjaDriver.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(BatchStageRunner.class);

    public BatchStageRunner(BatchStage batchStage) {
        this.batchStage = batchStage;
        this.writer = batchStage.getExportWriter();
        this.mutex = ((ExportWriter) writer).getMutex();
    }

    @Override
    public void start() throws TransformException {
        Task task = batchStage.getTask();
        if (task.isPythonTask()) {
            pemjaWorker = driver.createWorker((PythonTask) task, writer);
        } else {
            logger.error("Batch task must be python task.");
            throw new CreateWorkerException("Only python task can create worker.");
        }
        writer = new PemjaWriter(pemjaWorker);
    }

    @Override
    public void run() throws WriteBatchException {
        CollectionWriter collectionWriter = (CollectionWriter) batchStage.getBeforeStage().getExportWriter();
        BatchData batchData = collectionWriter.getCollectedData();

        mutex.lock();
        writer.writeBatch(batchData);

        // wait for py work finish writing.
        mutex.lock();
    }

    @Override
    public void close() {

    }
}
