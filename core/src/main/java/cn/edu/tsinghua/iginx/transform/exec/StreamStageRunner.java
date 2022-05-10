package cn.edu.tsinghua.iginx.transform.exec;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.api.Runner;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.driver.PythonDriver;
import cn.edu.tsinghua.iginx.transform.driver.Worker;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;
import cn.edu.tsinghua.iginx.transform.pojo.IginXTask;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import cn.edu.tsinghua.iginx.transform.pojo.StreamStage;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import cn.edu.tsinghua.iginx.transform.utils.Mutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StreamStageRunner implements Runner {

    private final StreamStage streamStage;

    private final int batchSize;

    private final Mutex mutex;

    private Writer writer;

    private Reader reader;

    private final List<Worker> workerList;

    private final PythonDriver driver = PythonDriver.getInstance();

    private final StatementExecutor executor = StatementExecutor.getInstance();

    private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private final static Logger logger = LoggerFactory.getLogger(StreamStageRunner.class);

    public StreamStageRunner(StreamStage stage) {
        this.streamStage = stage;
        this.batchSize = config.getBatchSize();
        this.workerList = new ArrayList<>();
        this.writer = streamStage.getExportWriter();
        this.mutex = ((ExportWriter) writer).getMutex();
    }

    @Override
    public void start() throws TransformException {
        List<Task> taskList = streamStage.getTaskList();
        for (int i = taskList.size() - 1; i >= 0; i--) {
            Task task = taskList.get(i);
            if (task.isPythonTask()) {
                try {
                    Worker worker = driver.createWorker((PythonTask) task, writer);
                    worker.start();
                    workerList.add(0, worker);
                    writer = new ArrowWriter(worker.getPyPort());
                } catch (TransformException e) {
                    logger.error("Steam stage runner fail to create worker");
                    throw e;
                }
            }
        }

        if (streamStage.isStartWithIginX()) {
            IginXTask firstTask = (IginXTask) streamStage.getTaskList().get(0);
            RowStream rowStream = getRowStream(streamStage.getSessionId(), firstTask.getSql());
            reader = new RowStreamReader(rowStream, batchSize);
        } else {
            CollectionWriter collectionWriter = (CollectionWriter) streamStage.getBeforeStage().getExportWriter();
            reader = new SplitReader(collectionWriter.getCollectedData(), batchSize);
        }
    }

    private RowStream getRowStream(long sessionId, String sql) {
        ExecuteStatementReq req = new ExecuteStatementReq(sessionId, sql);
        RequestContext context = contextBuilder.build(req);
        executor.execute(context);
        return context.getResult().getResultStream();
    }

    @Override
    public void run() throws WriteBatchException {
        while (reader.hasNextBatch()) {
            mutex.lock();
            BatchData batchData = reader.loadNextBatch();
            writer.writeBatch(batchData);
        }

        // wait for last batch finished.
        mutex.lock();
    }

    @Override
    public void close() {
        reader.close();
        workerList.forEach(Worker::close);
    }
}
