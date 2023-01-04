package cn.edu.tsinghua.iginx.parquet.policy;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TIME;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaiveParquetStoragePolicy implements ParquetStoragePolicy {

    private static final Logger logger = LoggerFactory.getLogger(NaiveParquetStoragePolicy.class);

    private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final String dataDir;

    private final Connection connection;

    private static final long FILE_SIZE_LIMIT_10MB = 10 * 1024 * 1024;  // byte

    private static final String SELECT_LAST_TIME_STMT = "SELECT time FROM '%s' order by time desc limit 1";

    public NaiveParquetStoragePolicy(String dataDir, Connection connection) {
        this.dataDir = dataDir;
        this.connection = connection;
    }

    @Override
    public Pair<Long, List<String>> getLatestPartition(String storageUnit) {
        Path duDirPath = Paths.get(dataDir, storageUnit);
        if (Files.notExists(duDirPath)) {
            createDir(duDirPath);
        }

        File duDir = duDirPath.toFile();
        File[] timePartitionDir = duDir.listFiles();

        if (timePartitionDir == null || timePartitionDir.length == 0) {
            // 初始化数据分区
            return initDataPartition(storageUnit);
        }

        File maxTimeDir = null;
        long maxTime = 0;
        for (File timeDir : timePartitionDir) {
            long curTime = Long.parseLong(timeDir.getName());
            if (curTime >= maxTime) {
                maxTime = curTime;
                maxTimeDir = timeDir;
            }
        }

        if (needCreateNewDataPartition(maxTimeDir)) {
            // 创建新数据分区
            return createNewDataPartition(storageUnit);
        }

        long startTime = Long.parseLong(maxTimeDir.getName());
        File[] pathPartition = maxTimeDir.listFiles();
        if (pathPartition == null || pathPartition.length == 0) {
            Pair<TimeSeriesRange, TimeInterval> duBoundary = metaManager.getBoundaryOfStorageUnit(storageUnit);
            return new Pair<>(startTime, Collections.singletonList(duBoundary.getK().getStartTimeSeries()));
        } else {
            List<String> startPaths = new ArrayList<>();
            for (File pathPartitionFile : pathPartition) {
                String startPath = pathPartitionFile.getName().split("__")[2];
                startPath = startPath.substring(0, startPath.lastIndexOf(".parquet"));
                startPaths.add(startPath);
            }
            return new Pair<>(startTime, startPaths);
        }
    }

    private boolean needCreateNewDataPartition(File file) {
        long fileSize = FileUtils.sizeOf(file);
        boolean needCreateNewDataPartition = fileSize > FILE_SIZE_LIMIT_10MB;
//        logger.info("current data file '{}' size: {} mb", file.getAbsolutePath(), fileSize / 1024 / 1024);
        if (needCreateNewDataPartition) {
            logger.info("current data file '{}', size: {} mb, create a new data partition",
                file.getAbsolutePath(), fileSize / 1024 / 1024);
        }
        return needCreateNewDataPartition;
    }

    private Pair<Long, List<String>> initDataPartition(String storageUnit) {
        lock.writeLock().lock();
        Pair<TimeSeriesRange, TimeInterval> duBoundary = metaManager.getBoundaryOfStorageUnit(storageUnit);
        long latestStartTime = duBoundary.getV().getStartTime();
        createDir(Paths.get(dataDir, storageUnit, String.valueOf(latestStartTime)));
        lock.writeLock().unlock();

        return new Pair<>(
            latestStartTime,
            Collections.singletonList(duBoundary.getK().getStartTimeSeries())
        );
    }

    private Pair<Long, List<String>> createNewDataPartition(String storageUnit) {
        // 切分新的数据分区
        lock.writeLock().lock();
        long latestStartTime = getLatestTime(storageUnit) + 1;
        createDir(Paths.get(dataDir, storageUnit, String.valueOf(latestStartTime)));
        Pair<TimeSeriesRange, TimeInterval> duBoundary = metaManager.getBoundaryOfStorageUnit(storageUnit);
        String startPath = duBoundary.getK().getStartTimeSeries();
        lock.writeLock().unlock();

        return new Pair<>(latestStartTime, Collections.singletonList(startPath));
    }

    private Long getLatestTime(String storageUnit) {
        Path path = Paths.get(dataDir, storageUnit, "*", "*.parquet");

        long latestTime = 0;
        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(SELECT_LAST_TIME_STMT, path.toString()));
            while (rs.next()) {
                latestTime = rs.getLong(COLUMN_TIME);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error("get latest time failed.");
        }
        return latestTime;
    }

    @Override
    public TreeMap<Long, List<String>> getAllPartition(String storageUnit) {
        lock.readLock().lock();
        File duDir = Paths.get(dataDir, storageUnit).toFile();
        File[] timePartitionDir = duDir.listFiles();

        TreeMap<Long, List<String>> res = new TreeMap<>();
        if (timePartitionDir != null && timePartitionDir.length != 0) {
            for (File tDir : timePartitionDir) {
                long startTime = Long.parseLong(tDir.getName());
                File[] pathPartition = tDir.listFiles();
                if (pathPartition != null && pathPartition.length != 0) {
                    for (File pathPartitionFile : pathPartition) {
                        String startPath = pathPartitionFile.getName().split("__")[2];
                        startPath = startPath.substring(0, startPath.lastIndexOf(".parquet"));
                        collectInMap(res, startTime, startPath);
                    }
                } else {
                    Pair<TimeSeriesRange, TimeInterval> duBoundary = metaManager.getBoundaryOfStorageUnit(storageUnit);
                    collectInMap(res, startTime, duBoundary.getK().getStartTimeSeries());
                }
            }
        } else {
            Pair<TimeSeriesRange, TimeInterval> duBoundary = metaManager.getBoundaryOfStorageUnit(storageUnit);
            collectInMap(res, duBoundary.getV().getStartTime(), duBoundary.getK().getStartTimeSeries());
        }
        lock.readLock().unlock();
        return res;
    }

    private void collectInMap(Map<Long, List<String>> map, long time, String path) {
        if (map.containsKey(time)) {
            map.get(time).add(path);
        } else {
            map.put(time, new ArrayList<>(Collections.singletonList(path)));
        }
    }

    private void createDir(Path path) {
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            logger.error("create dir {} failed", path);
        }
    }

    private void createFile(Path path) {
        try {
            Files.createFile(path);
        } catch (IOException e) {
            logger.error("create file {} failed", path);
        }
    }

    @Override
    public FlushType getFlushType(Path outputFilePath, long pointsNum) {
        return FlushType.ORIGIN;
    }
}
