package cn.edu.tsinghua.iginx.parquet.exec;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_NAME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TIME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TYPE;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DATATYPE_BIGINT;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.DUCKDB_SCHEMA;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.NAME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.PARQUET_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.fromDuckDBDataType;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.toParquetDataType;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.MergeTimeRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.parquet.entity.ParquetQueryRowStream;
import cn.edu.tsinghua.iginx.parquet.entity.WritePlan;
import cn.edu.tsinghua.iginx.parquet.policy.ParquetStoragePolicy;
import cn.edu.tsinghua.iginx.parquet.policy.ParquetStoragePolicy.FlushType;
import cn.edu.tsinghua.iginx.parquet.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.parquet.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalExecutor implements Executor {

    private static final Logger logger = LoggerFactory.getLogger(LocalExecutor.class);

    // data-startTime-startPath
    private static final String DATA_FILE_NAME_FORMATTER = "data__%s__%s.parquet";

    private static final String APPENDIX_DATA_FILE_NAME_FORMATTER = "appendixData__%s__%s.parquet";

    private static final String CREATE_TABLE_STMT = "CREATE TABLE %s (%s)";

    private static final String INSERT_STMT_PREFIX = "INSERT INTO %s(%s) VALUES ";

    private static final String CREATE_TABLE_FROM_PARQUET_STMT = "CREATE TABLE %s AS SELECT * FROM '%s'";

    private static final String ADD_COLUMNS_STMT = "ALTER TABLE %s ADD COLUMN %s %s";

    private static final String DESCRIBE_STMT = "DESCRIBE %s";

    private static final String SAVE_TO_PARQUET_STMT = "COPY %s TO '%s' (FORMAT 'parquet')";

    private static final String DROP_TABLE_STMT = "DROP TABLE %s";

    private static final String SELECT_STMT = "SELECT time, %s FROM '%s' WHERE %s ORDER BY time";

    private static final String SELECT_TIME_STMT = "SELECT time FROM '%s' ORDER BY time";

    private static final String SELECT_FIRST_TIME_STMT = "SELECT time FROM '%s' order by time limit 1";

    private static final String SELECT_LAST_TIME_STMT = "SELECT time FROM '%s' order by time desc limit 1";

    private static final String SELECT_PARQUET_SCHEMA = "SELECT * FROM parquet_schema('%s')";

    private static final String DELETE_DATA_STMT = "UPDATE %s SET %s=NULL WHERE time >= %s AND time <= %s";

    private static final String DROP_COLUMN_STMT = "ALTER TABLE %s DROP %s";

    private final ParquetStoragePolicy policy;

    private final Connection connection;

    public final String dataDir;

    public LocalExecutor(ParquetStoragePolicy policy, Connection connection, String dataDir) {
        this.policy = policy;
        this.connection = connection;
        this.dataDir = dataDir;
    }

    @Override
    public TaskExecuteResult executeProjectTask(List<String> paths, TagFilter tagFilter,
        String filter, String storageUnit, boolean isDummyStorageUnit) {
        try {
            createDUDirectoryIfNotExists(storageUnit);
        } catch (PhysicalException e) {
            return new TaskExecuteResult(e);
        }

        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();

            StringBuilder builder = new StringBuilder();
            List<String> pathList = determinePathListWithTagFilter(storageUnit, paths, tagFilter, isDummyStorageUnit);
            if (pathList.isEmpty()) {
                RowStream rowStream = new ClearEmptyRowStreamWrapper(
                    ParquetQueryRowStream.EMPTY_PARQUET_ROW_STREAM
                );
                return new TaskExecuteResult(rowStream);
            }
            pathList.forEach(path -> builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", "));
            Path path = Paths.get(dataDir, storageUnit, "*", "*.parquet");
            if (isDummyStorageUnit) {
                path = Paths.get(dataDir, "*.parquet");
            }

            ResultSet rs = stmt.executeQuery(
                String.format(SELECT_STMT,
                    builder.toString(),
                    path.toString(),
                    filter));
            stmt.close();
            conn.close();

            RowStream rowStream = new ClearEmptyRowStreamWrapper(
                new MergeTimeRowStreamWrapper(new ParquetQueryRowStream(rs, tagFilter)));
            return new TaskExecuteResult(rowStream);
        } catch (SQLException | PhysicalException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in parquet failure", e));
        }
    }

    private List<String> determinePathListWithTagFilter(String storageUnit, List<String> patterns,
        TagFilter tagFilter, boolean isDummyStorageUnit) throws PhysicalException {
        if (tagFilter == null) {
            return determinePathList(storageUnit, patterns, isDummyStorageUnit);
        }

        List<Timeseries> timeSeries = new ArrayList<>();
        if (isDummyStorageUnit) {
            timeSeries.addAll(getTimeSeriesOfDir(Paths.get(dataDir, "*.parquet")));
        } else {
            timeSeries.addAll(getTimeSeriesOfStorageUnit(storageUnit));
        }

        List<String> pathList = new ArrayList<>();
        for (Timeseries ts: timeSeries) {
            for (String pattern : patterns) {
                if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath()) &&
                    TagKVUtils.match(ts.getTags(), tagFilter)) {
                    String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
                    pathList.add(path);
                    break;
                }
            }
        }
        return pathList;
    }

    private List<String> determinePathList(String storageUnit, List<String> patterns,
        boolean isDummyStorageUnit) throws PhysicalException {
        Set<String> patternWithoutStarSet = new HashSet<>();
        List<String> patternWithStarList = new ArrayList<>();
        patterns.forEach(pattern -> {
            if (pattern.contains("*")) {
                patternWithStarList.add(pattern);
            } else {
                patternWithoutStarSet.add(pattern);
            }
        });

        List<String> pathList = new ArrayList<>();
        List<Timeseries> timeSeries = new ArrayList<>();
        if (isDummyStorageUnit) {
            timeSeries.addAll(getTimeSeriesOfDir(Paths.get(dataDir, "*.parquet")));
        } else {
            timeSeries.addAll(getTimeSeriesOfStorageUnit(storageUnit));
        }
        for (Timeseries ts : timeSeries) {
            if (patternWithoutStarSet.contains(ts.getPath())) {
                String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
                pathList.add(path);
                continue;
            }
            for (String pattern : patternWithStarList) {
                if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath())) {
                    String path = TagKVUtils.toFullName(ts.getPath(), ts.getTags());
                    pathList.add(path);
                    break;
                }
            }
        }
        return pathList;
    }

    @Override
    public TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit) {
        try {
            createDUDirectoryIfNotExists(storageUnit);
        } catch (PhysicalException e) {
            return new TaskExecuteResult(e);
        }

        DataViewWrapper data = new DataViewWrapper(dataView);
        List<WritePlan> writePlans = getWritePlans(data, storageUnit);
        for (WritePlan writePlan : writePlans) {
            try {
                executeWritePlan(data, writePlan);
            } catch (SQLException e) {
                logger.error("execute row write plan error", e);
                return new TaskExecuteResult(null, new PhysicalException("execute insert task in parquet failure", e));
            }
        }
        return new TaskExecuteResult(null, null);
    }

    private void executeWritePlan(DataViewWrapper data, WritePlan writePlan) throws SQLException {
        Connection conn = ((DuckDBConnection) connection).duplicate();
        Statement stmt = conn.createStatement();
        Path path = writePlan.getFilePath();
        String filename = writePlan.getFilePath().getFileName().toString();
        String tableName = filename.substring(0, filename.lastIndexOf(".")).replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);

        // prepare to write data.
        if (!Files.exists(path)) {
            String createTableStmt = generateCreateTableStmt(data, writePlan, tableName);
            stmt.execute(createTableStmt);
        } else {
            stmt.execute(String.format(CREATE_TABLE_FROM_PARQUET_STMT, tableName, path.toString()));
            ResultSet rs = stmt.executeQuery(String.format(DESCRIBE_STMT, tableName));
            Set<String> existsColumns = new HashSet<>();
            while (rs.next()) {
                existsColumns.add((String) rs.getObject(COLUMN_NAME));
            }
            List<String> addColumnsStmts = generateAddColumnsStmt(data, writePlan, tableName, existsColumns);
            if (!addColumnsStmts.isEmpty()) {
                for (String addColumnsStmt : addColumnsStmts) {
                    stmt.execute(addColumnsStmt);
                }
            }
        }

        // write data
        String insertPrefix = generateInsertStmtPrefix(data, writePlan, tableName);
        String insertBody;
        switch (data.getRawDataType()) {
            case Column:
            case NonAlignedColumn:
                insertBody = generateColInsertStmtBody(data, writePlan);
                break;
            case Row:
            case NonAlignedRow:
            default:
                insertBody = generateRowInsertStmtBody(data, writePlan);
                break;
        }
        stmt.execute(insertPrefix + insertBody);

        // save to file
        stmt.execute(String.format(SAVE_TO_PARQUET_STMT, tableName, path.toString()));

        stmt.execute(String.format(DROP_TABLE_STMT, tableName));
        stmt.close();
        conn.close();
    }

    private String generateRowInsertStmtBody(DataViewWrapper data, WritePlan writePlan) {
        StringBuilder builder = new StringBuilder();

        int startPathIdx = data.getPathIndex(writePlan.getPathList().get(0));
        int endPathIdx = data.getPathIndex(writePlan.getPathList().get(writePlan.getPathList().size() - 1));
        int startTimeIdx = data.getTimestampIndex(writePlan.getTimeInterval().getStartTime());
        int endTimeIdx = data.getTimestampIndex(writePlan.getTimeInterval().getEndTime());

        for (int i = startTimeIdx; i <= endTimeIdx; i++) {
            BitmapView bitmapView = data.getBitmapView(i);
            builder.append("(").append(data.getTimestamp(i)).append(", ");

            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    if (startPathIdx <= j && j <= endPathIdx) {
                        if (data.getDataType(j) == DataType.BINARY) {
                            builder.append("'").append(new String((byte[]) data.getValue(i, index))).append("', ");
                        } else {
                            builder.append(data.getValue(i, index)).append(", ");
                        }
                    }
                    index++;
                } else {
                    if (startPathIdx <= j && j <= endPathIdx) {
                        builder.append("NULL, ");
                    }
                }
            }
            builder.append("), ");
        }
        return builder.toString();
    }

    private String generateColInsertStmtBody(DataViewWrapper data, WritePlan writePlan) {
        int startPathIdx = data.getPathIndex(writePlan.getPathList().get(0));
        int endPathIdx = data.getPathIndex(writePlan.getPathList().get(writePlan.getPathList().size() - 1));
        int startTimeIdx = data.getTimestampIndex(writePlan.getTimeInterval().getStartTime());
        int endTimeIdx = data.getTimestampIndex(writePlan.getTimeInterval().getEndTime());

        String[] rowValueArray = new String[endTimeIdx - startTimeIdx + 1];
        for (int i = startTimeIdx; i <= endTimeIdx; i++) {
            rowValueArray[i] = "(" + data.getTimestamp(i) + ", ";
        }
        for (int i = startPathIdx; i <= endPathIdx; i++) {
            BitmapView bitmapView = data.getBitmapView(i);

            int index = 0;
            for (int j = 0; j < data.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    if (startTimeIdx <= j && j <= endTimeIdx) {
                        if (data.getDataType(i) == DataType.BINARY) {
                            rowValueArray[j - startPathIdx] += "'" + new String((byte[]) data.getValue(i, index)) + "', ";
                        } else {
                            rowValueArray[j - startPathIdx] += data.getValue(i, index) + ", ";
                        }
                    }
                    index++;
                } else {
                    if (startTimeIdx <= j && j <= endTimeIdx) {
                        rowValueArray[j - startPathIdx] += "NULL, ";
                    }
                }
            }
        }
        for (int i = startTimeIdx; i <= endTimeIdx; i++) {
            rowValueArray[i] +=  "), ";
        }

        StringBuilder builder = new StringBuilder();
        for (String row : rowValueArray) {
            builder.append(row);
        }
        return builder.toString();
    }

    private List<String> generateAddColumnsStmt(DataViewWrapper data, WritePlan writePlan,
        String tableName, Set<String> existsColumns) {
        List<String> addColumnStmts = new ArrayList<>();

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i).replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
            String type = toParquetDataType(data.getDataType(i));
            if (writePlan.getPathList().contains(path.replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR))
                && !existsColumns.contains(path)) {
                builder.append("{path: ").append(path).append(", type: ").append(type).append("} ");
                addColumnStmts.add(String.format(ADD_COLUMNS_STMT, tableName, path, type));
            }
        }
        logger.info("add columns: {}", builder.toString());

        return addColumnStmts;
    }

    private String generateInsertStmtPrefix(DataViewWrapper data, WritePlan writePlan, String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("time, ");
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            if (writePlan.getPathList().contains(path)) {
                builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR)).append(", ");
            }
        }
        builder.deleteCharAt(builder.length() - 2);
        String columns = builder.toString();
        String insertStmtPrefix = String.format(INSERT_STMT_PREFIX, tableName, columns);
        logger.info("InsertStmtPrefix: {}", insertStmtPrefix);
        return insertStmtPrefix;
    }

    private String generateCreateTableStmt(DataViewWrapper data, WritePlan writePlan, String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(COLUMN_TIME).append(" ").append(DATATYPE_BIGINT).append(", ");
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            if (writePlan.getPathList().contains(path)) {
                builder.append(path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR))
                    .append(" ")
                    .append(toParquetDataType(data.getDataType(i)))
                    .append(", ");
            }
        }
        builder.deleteCharAt(builder.length() - 2);
        String columns = builder.toString();
        String createTableStmt = String.format(CREATE_TABLE_STMT, tableName, columns);
        logger.info("CreateTableStmt: {}", createTableStmt);
        return createTableStmt;
    }

    private List<WritePlan> getWritePlans(DataViewWrapper data, String storageUnit) {
        if (data.getTimeSize() == 0) {  // empty data section
            return new ArrayList<>();
        }
        TimeInterval timeInterval = new TimeInterval(
            data.getTimestamp(0),
            data.getTimestamp(data.getTimeSize() - 1)
        );

        Pair<Long, List<String>> latestPartition = policy.getLatestPartition(storageUnit);
        List<WritePlan> writePlans = new ArrayList<>();

        if (timeInterval.getStartTime() >= latestPartition.getK()) {
            // 所有写入数据位于最新的分区
            List<String> tsPartition = new ArrayList<>(latestPartition.getV());
            tsPartition.add(null);
            for (int i = 0; i < tsPartition.size() - 1; i++) {
                List<String> pathList = new ArrayList<>();
                String startPath = tsPartition.get(i);
                String endPath = tsPartition.get(i + 1);
                for (int j = 0; j < data.getPathNum(); j++) {
                    String path = data.getPath(j);
                    if (StringUtils.compare(path, startPath, true) < 0) {
                        continue;
                    }
                    if (StringUtils.compare(path, endPath, false) >= 0) {
                        break;
                    }
                    pathList.add(path);
                }
                if (!pathList.isEmpty()) {
                    Path path = Paths.get(dataDir, storageUnit,
                        String.valueOf(latestPartition.getK()),
                        String.format(DATA_FILE_NAME_FORMATTER, latestPartition.getK(), startPath));

                    long pointNum = pathList.size() * (timeInterval.getEndTime() - timeInterval.getStartTime());
                    if (policy.getFlushType(path, pointNum).equals(FlushType.APPENDIX)) {
                        path = Paths.get(dataDir, storageUnit,
                            String.valueOf(latestPartition.getK()),
                            String.format(APPENDIX_DATA_FILE_NAME_FORMATTER, latestPartition.getK(),
                                startPath));
                    }
                    writePlans.add(new WritePlan(path, pathList, timeInterval));

                }
            }
        } else {
            // 有老分区数据需要写入
            TreeMap<Long, List<String>> allPartition = policy.getAllPartition(storageUnit);
            List<Long> timePartition = new ArrayList<>(allPartition.keySet());
            timePartition.add(Long.MAX_VALUE);
            for (int i = 0; i < timePartition.size() - 1; i++) {
                long startTime = timePartition.get(i);
                long endTime = timePartition.get(i + 1);
                TimeInterval partTimeInterval = new TimeInterval(startTime, endTime);
                if (timeInterval.isIntersect(partTimeInterval)) {
                    TimeInterval timeIntersect = timeInterval.getIntersectWithLCRO(partTimeInterval);
                    List<String> tsPartition = new ArrayList<>(allPartition.get(startTime));
                    tsPartition.add(null);
                    for (int j = 0; j < tsPartition.size() - 1; j++) {
                        List<String> pathList = new ArrayList<>();
                        String startPath = tsPartition.get(j);
                        String endPath = tsPartition.get(j + 1);
                        for (int k = 0; k < data.getPathNum(); k++) {
                            String path = data.getPath(k);
                            if (StringUtils.compare(path, startPath, true) < 0) {
                                continue;
                            }
                            if (StringUtils.compare(path, endPath, false) >= 0) {
                                break;
                            }
                            pathList.add(path);
                        }
                        if (!pathList.isEmpty()) {
                            Path path = Paths.get(dataDir, storageUnit,
                                String.valueOf(partTimeInterval.getStartTime()),
                                String.format(
                                    DATA_FILE_NAME_FORMATTER,
                                    partTimeInterval.getStartTime(),
                                    startPath));

                            long pointNum = pathList.size() * (timeInterval.getEndTime() - timeInterval.getStartTime());
                            if (policy.getFlushType(path, pointNum).equals(FlushType.APPENDIX)) {
                                path = Paths.get(dataDir, storageUnit,
                                    String.valueOf(latestPartition.getK()),
                                    String.format(APPENDIX_DATA_FILE_NAME_FORMATTER, latestPartition.getK(),
                                        startPath));
                            }
                            writePlans.add(new WritePlan(path, pathList, timeIntersect));
                        }
                    }
                }
            }
        }
        return writePlans;
    }

    @Override
    public TaskExecuteResult executeDeleteTask(List<String> paths, List<TimeRange> timeRanges, TagFilter tagFilter, String storageUnit) {
        try {
            createDUDirectoryIfNotExists(storageUnit);
        } catch (PhysicalException e) {
            return new TaskExecuteResult(e);
        }

        if (timeRanges == null || timeRanges.size() == 0) { // 没有传任何 time range
            if (paths.size() == 1 && paths.get(0).equals("*") && tagFilter == null) {
                File duDir = Paths.get(dataDir, storageUnit).toFile();
                deleteFile(duDir);
            } else {
                List<String> deletedPaths;
                try {
                    deletedPaths = determinePathListWithTagFilter(storageUnit, paths, tagFilter, false);
                } catch (PhysicalException e) {
                    logger.warn("encounter error when delete path: " + e.getMessage());
                    return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute delete path task in parquet failure", e));
                }
                for (String path : deletedPaths) {
                    deleteDataInAllFiles(storageUnit, path, null);
                }
            }
        } else {
            try {
                List<String> deletedPaths = determinePathListWithTagFilter(storageUnit, paths, tagFilter, false);
                for (String path : deletedPaths) {
                    deleteDataInAllFiles(storageUnit, path, timeRanges);
                }
            } catch (PhysicalException e) {
                logger.error("encounter error when delete data: " + e.getMessage());
                return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute delete data task in parquet failure", e));
            }
        }
        return new TaskExecuteResult(null, null);
    }

    private void deleteDataInAllFiles(String storageUnit, String path, List<TimeRange> timeRanges) {
        Path duDir = Paths.get(dataDir, storageUnit);
        if (Files.notExists(duDir)) {
            return;
        }
        File[] dirs = duDir.toFile().listFiles();
        if (dirs != null) {
            for (File dir : dirs) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        deleteDataInFile(file, path, timeRanges);
                    }
                }
            }
        }
    }

    private void deleteDataInFile(File file, String path, List<TimeRange> timeRanges) {
        path = path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, file.getAbsolutePath()));
            boolean hasPath = false;
            while (rs.next()) {
                String pathName = (String) rs.getObject(NAME);
                if (pathName.equals(path)) {
                    hasPath = true;
                    break;
                }
            }
            if (hasPath) {
                String filename = file.getName();
                String tableName = filename.substring(0, filename.lastIndexOf(".")).replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
                // 1.load 2.drop or delete 3.write back
                stmt.execute(String.format(CREATE_TABLE_FROM_PARQUET_STMT, tableName, file.getAbsolutePath()));
                if (timeRanges == null) {
                    stmt.execute(String.format(DROP_COLUMN_STMT, tableName, path));
                } else {
                    for (TimeRange timeRange : timeRanges) {
                        stmt.execute(String.format(DELETE_DATA_STMT, tableName, path, timeRange.getActualBeginTime(), timeRange.getActualEndTime()));
                    }
                }
                stmt.execute(String.format(SAVE_TO_PARQUET_STMT, tableName, file.getAbsolutePath()));
                stmt.execute(String.format(DROP_TABLE_STMT, tableName));
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            logger.error("delete path failure.", e);
        }
    }

    private boolean deleteFile(File file) {
        if (!file.exists()) {
            return false;
        }
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteFile(f);
                }
            }
        }
        return file.delete();
    }

    private void createDUDirectoryIfNotExists(String storageUnit) throws PhysicalException {
        try {
            if (Files.notExists(Paths.get(dataDir, storageUnit))) {
                Files.createDirectories(Paths.get(dataDir, storageUnit));
            }
        } catch (IOException e) {
            throw new PhysicalException("fail to create du dir");
        }
    }

    @Override
    public List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit) throws PhysicalException {
        Path path = Paths.get(dataDir, storageUnit, /*timePartition*/"*", /*pathPartition*/"*.parquet");
        return getTimeSeriesOfDir(path);
    }

    private List<Timeseries> getTimeSeriesOfDir(Path path) throws PhysicalTaskExecuteFailureException {
        Set<Timeseries> timeseries = new HashSet<>();
        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, path.toString()));
            while (rs.next()) {
                String pathName = ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(pathName);
                DataType type = fromDuckDBDataType((String) rs.getObject(COLUMN_TYPE));
                if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_TIME)) {
                    timeseries.add(new Timeseries(pair.k, type, pair.v));
                }
            }
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            if (e.getMessage().contains("No files found that match the pattern")) {
                return new ArrayList<>(timeseries);
            }
            throw new PhysicalTaskExecuteFailureException("get time series failure", e);
        }
        return new ArrayList<>(timeseries);
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage() throws PhysicalException {
        File rootDir = new File(dataDir);
        List<String> parquetFiles = new ArrayList<>();
        findParquetFiles(parquetFiles, rootDir);

        long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;
        TreeSet<String> pathTreeSet = new TreeSet<>();
        for (String filepath : parquetFiles) {
            Pair<Set<String>, Pair<Long, Long>> ret = getBoundaryOfSingleFile(filepath);
            if (ret != null) {
                pathTreeSet.addAll(ret.getK());
                startTime = Math.min(startTime, ret.getV().getK());
                endTime = Math.max(endTime, ret.getV().getV());
            }
        }

        startTime = startTime == Long.MAX_VALUE ? 0 : startTime;
        endTime = endTime == Long.MIN_VALUE ? Long.MAX_VALUE : endTime;

        if (!pathTreeSet.isEmpty()) {
            return new Pair<>(
                new TimeSeriesInterval(pathTreeSet.first(), pathTreeSet.last()),
                new TimeInterval(startTime, endTime));
        } else {
            return new Pair<>(new TimeSeriesInterval(null, null), new TimeInterval(startTime, endTime));
        }
    }

    private Pair<Set<String>, Pair<Long, Long>> getBoundaryOfSingleFile(String filepath)
        throws PhysicalTaskExecuteFailureException {
        Path path = Paths.get(filepath);
        if (Files.notExists(path)) {
            return null;
        }

        long startTime = 0, endTime = Long.MAX_VALUE;
        Set<String> pathSet = new HashSet<>();
        try {
            Connection conn = ((DuckDBConnection) connection).duplicate();
            Statement stmt = conn.createStatement();

            ResultSet firstTimeRS = stmt.executeQuery(String.format(SELECT_FIRST_TIME_STMT, path.toString()));
            while (firstTimeRS.next()) {
                startTime = firstTimeRS.getLong(COLUMN_TIME);
            }
            firstTimeRS.close();

            ResultSet lastTimeRS = stmt.executeQuery(String.format(SELECT_LAST_TIME_STMT, path.toString()));
            while (lastTimeRS.next()) {
                endTime = lastTimeRS.getLong(COLUMN_TIME);
            }
            lastTimeRS.close();

            ResultSet rs = stmt.executeQuery(String.format(SELECT_PARQUET_SCHEMA, path.toString()));
            while (rs.next()) {
                String pathName = ((String) rs.getObject(NAME)).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
                if (!pathName.equals(DUCKDB_SCHEMA) && !pathName.equals(COLUMN_TIME)) {
                    pathSet.add(pathName);
                }
            }
            rs.close();

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            throw new PhysicalTaskExecuteFailureException("get boundary of file failure: " + filepath);
        }
        return new Pair<>(pathSet, new Pair<>(startTime, endTime));
    }

    private void findParquetFiles(List<String> parquetFiles, File file) {
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        for (File subFile : files) {
            if (subFile.isDirectory()) {
                findParquetFiles(parquetFiles, subFile);
            }
            if (subFile.getName().endsWith(".parquet")) {
                parquetFiles.add(subFile.getPath());
            }
        }
    }

    @Override
    public void close() throws PhysicalException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
    }
}
