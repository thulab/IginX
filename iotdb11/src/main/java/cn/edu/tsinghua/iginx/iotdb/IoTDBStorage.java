package cn.edu.tsinghua.iginx.iotdb;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class IoTDBStorage implements IStorage {

    private final SessionPool sessionPool;

    private static final Logger logger = LoggerFactory.getLogger(IoTDBStorage.class);

    public IoTDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        if (!meta.getStorageEngine().equals("iotdb11")) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection(meta)) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        sessionPool = createSessionPool(meta);
    }

    private boolean testConnection(StorageEngineMeta meta) {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");

        Session session = new Session(meta.getIp(), meta.getPort(), username, password);

        try {
            session.open(false);
            session.close();
        } catch (IoTDBConnectionException e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    private SessionPool createSessionPool(StorageEngineMeta meta) {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");
        int sessionPoolSize = Integer.parseInt(extraParams.getOrDefault("sessionPoolSize", "100"));
        return new SessionPool(meta.getIp(), meta.getPort(), username, password, sessionPoolSize);
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) throws PhysicalException {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            throw new NonExecutablePhysicalTaskException("unsupported physical task");
            // return new TaskExecuteResult();
        }
        Operator op = operators.get(0);

        switch (op.getType()) {
            case Project: // 目前只实现 project 操作符
                Project project = (Project) op;
                break;
            default:
                break;
        }

        return null;
    }

    public RowStream executeProject(Project project, )

}
