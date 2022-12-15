package cn.edu.tsinghua.iginx.parquet;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.parquet.exec.Executor;
import cn.edu.tsinghua.iginx.parquet.exec.LocalExecutor;
import cn.edu.tsinghua.iginx.parquet.exec.RemoteExecutor;
import cn.edu.tsinghua.iginx.parquet.policy.NaiveParquetStoragePolicy;
import cn.edu.tsinghua.iginx.parquet.policy.ParquetStoragePolicy;
import cn.edu.tsinghua.iginx.parquet.server.ParquetServer;
import cn.edu.tsinghua.iginx.parquet.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(ParquetStorage.class);

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final String DRIVER_NAME = "org.duckdb.DuckDBDriver";

    private static final String CONN_URL = "jdbc:duckdb:";

    private Executor executor;

    public ParquetStorage(StorageEngineMeta meta) throws StorageInitializationException {
        boolean isLocal = config.isLocalParquetStorage();
        if (isLocal) {
            initLocalStorage(meta);
        } else {
            initRemoteStorage(meta);
        }
    }

    private void initLocalStorage(StorageEngineMeta meta) throws StorageInitializationException {
        if (!testLocalConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }

        Map<String, String> extraParams = meta.getExtraParams();
        String dataDir = extraParams.get("dir");
        try {
            if (Files.notExists(Paths.get(dataDir))) {
                Files.createDirectories(Paths.get(dataDir));
            }
        } catch (IOException e) {
            throw new StorageInitializationException("encounter error when create data dir");
        }

        Connection connection;
        try {
            connection = DriverManager.getConnection(CONN_URL);
        } catch (SQLException e) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }

        ParquetStoragePolicy policy = new NaiveParquetStoragePolicy(dataDir, connection);

        this.executor = new LocalExecutor(policy, connection, dataDir);

        new Thread(new ParquetServer(meta.getPort(), executor)).start();
    }

    private void initRemoteStorage(StorageEngineMeta meta) throws StorageInitializationException {
        try {
            this.executor = new RemoteExecutor(meta.getIp(), meta.getPort());
        } catch (TTransportException e) {
            throw new StorageInitializationException("encounter error when init RemoteStorage " + e.getMessage());
        }
    }

    private boolean testLocalConnection() {
        try {
            Class.forName(DRIVER_NAME);
            Connection conn = DriverManager.getConnection(CONN_URL);
            conn.close();
            return true;
        } catch (ClassNotFoundException | SQLException e) {
            return false;
        }
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(
                new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        String storageUnit = task.getStorageUnit();
        Operator op = operators.get(0);

        boolean isDummyStorageUnit = task.isDummyStorageUnit();
        if (op.getType() == OperatorType.Project) {
            Project project = (Project) op;
            Filter filter;
            if (operators.size() == 2) {
                filter = ((Select) operators.get(1)).getFilter();
            } else {
                filter = new AndFilter(Arrays.asList(
                    new TimeFilter(Op.GE, fragment.getTimeInterval().getStartTime()),
                    new TimeFilter(Op.L, fragment.getTimeInterval().getEndTime())));
            }
            return executor.executeProjectTask(
                project.getPatterns(),
                project.getTagFilter(),
                FilterTransformer.toString(filter),
                storageUnit,
                isDummyStorageUnit);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executor.executeInsertTask(
                insert.getData(),
                storageUnit);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executor.executeDeleteTask(
                delete.getPatterns(),
                delete.getTimeRanges(),
                delete.getTagFilter(),
                storageUnit);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        return executor.getTimeSeriesOfStorageUnit("*");
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        return executor.getBoundaryOfStorage();
    }

    @Override
    public void release() throws PhysicalException {
        executor.close();
    }
}
