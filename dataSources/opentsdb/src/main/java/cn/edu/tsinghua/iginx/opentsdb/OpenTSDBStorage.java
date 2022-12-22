package cn.edu.tsinghua.iginx.opentsdb;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.Connector;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.ClearEmptyRowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.opentsdb.query.entity.OpenTSDBRowStream;
import cn.edu.tsinghua.iginx.opentsdb.query.entity.OpenTSDBSchema;
import cn.edu.tsinghua.iginx.opentsdb.tools.DataViewWrapper;
import cn.edu.tsinghua.iginx.opentsdb.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.apache.http.nio.reactor.IOReactorException;
import org.opentsdb.client.OpenTSDBClient;
import org.opentsdb.client.OpenTSDBClientFactory;
import org.opentsdb.client.OpenTSDBConfig;
import org.opentsdb.client.bean.request.Point;
import org.opentsdb.client.bean.request.Query;
import org.opentsdb.client.bean.request.SubQuery;
import org.opentsdb.client.bean.request.SuggestQuery;
import org.opentsdb.client.bean.response.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static cn.edu.tsinghua.iginx.opentsdb.tools.DataTypeTransformer.DATA_TYPE;
import static cn.edu.tsinghua.iginx.opentsdb.tools.DataTypeTransformer.fromOpenTSDB;

public class OpenTSDBStorage implements IStorage {

    private static final String STORAGE_ENGINE = "opentsdb";

    private static final String DU_PREFIX = "unit";

    private static final int HTTP_CONNECT_POOL_SIZE = 100;

    private static final int HTTP_CONNECT_TIMEOUT = 100;

    private final OpenTSDBClient client;

    private final StorageEngineMeta meta;

    private static final Logger logger = LoggerFactory.getLogger(OpenTSDBStorage.class);

    public OpenTSDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        Map<String, String> extraParams = meta.getExtraParams();
        String url = extraParams.getOrDefault("url", "http://127.0.0.1");

        OpenTSDBConfig config = OpenTSDBConfig
            .address(url, meta.getPort())
            .httpConnectionPool(HTTP_CONNECT_POOL_SIZE)
            .httpConnectTimeout(HTTP_CONNECT_TIMEOUT)
            .config();
        try {
            client = OpenTSDBClientFactory.connect(config);
        } catch (IOReactorException e) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        logger.info(meta + " is initialized.");
    }

    private boolean testConnection() {
        Map<String, String> extraParams = meta.getExtraParams();
        String url = extraParams.get("url");
        OpenTSDBConfig config = OpenTSDBConfig.address(url, meta.getPort()).config();
        try {
            OpenTSDBClient client = OpenTSDBClientFactory.connect(config);
            client.gracefulClose();
        } catch (IOException e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public Connector getConnector() {
        return null;
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();

        boolean isDummyStorageUnit = task.isDummyStorageUnit();
        if (op.getType() == OperatorType.Project) {
            Project project = (Project) op;
            return isDummyStorageUnit ? executeProjectHistoryTask(fragment.getTimeInterval(), storageUnit, project) : executeProjectTask(fragment.getTimeInterval(), storageUnit, project);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(storageUnit, delete);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
        List<OpenTSDBSchema> schemas = null;
        try {
            schemas = determineDeletePathList(storageUnit, delete);
        } catch (PhysicalException e) {
            logger.error("encounter error when delete data in opentsdb: ", e);
        }
        if (schemas == null || schemas.size() == 0) {
            return new TaskExecuteResult(null, null);
        }

        if (delete.getTimeRanges() == null || delete.getTimeRanges().size() == 0) { // 没有传任何 time range
            for (OpenTSDBSchema schema : schemas) {
                Query query = Query
                    .begin(0L)
                    .end(Long.MAX_VALUE)
                    .sub(SubQuery.metric(schema.getMetric()).aggregator(SubQuery.Aggregator.NONE).build())
                    .msResolution()
                    .build();
                try {
                    client.delete(query);
                } catch (Exception e) {
                    logger.error("encounter error when delete data in opentsdb: ", e);
                }
            }
            return new TaskExecuteResult(null, null);
        }
        // 删除某些序列的某一段数据
        for (OpenTSDBSchema schema : schemas) {
            for (TimeRange timeRange : delete.getTimeRanges()) {
                Query query = Query
                    .begin(timeRange.getActualBeginTime())
                    .end(timeRange.getActualEndTime())
                    .sub(SubQuery.metric(schema.getMetric()).aggregator(SubQuery.Aggregator.NONE).build())
                    .msResolution()
                    .build();
                try {
                    client.delete(query);
                } catch (Exception e) {
                    logger.error("encounter error when delete data in opentsdb: ", e);
                }
            }
        }
        return new TaskExecuteResult(null, null);
    }

    private List<OpenTSDBSchema> determineDeletePathList(String storageUnit, Delete delete) throws PhysicalException {
        List<OpenTSDBSchema> schemas = new ArrayList<>();

        if (delete.getTagFilter() == null) {
            delete.getPatterns().forEach(pattern -> schemas.add(new OpenTSDBSchema(pattern, storageUnit)));
        } else {
            List<String> patterns = delete.getPatterns();
            TagFilter tagFilter = delete.getTagFilter();
            List<Timeseries> timeSeries = getTimeSeries();

            for (Timeseries ts: timeSeries) {
                for (String pattern : patterns) {
                    if (Pattern.matches(StringUtils.reformatPath(pattern), ts.getPath()) &&
                        TagKVUtils.match(ts.getTags(), tagFilter)) {
                        schemas.add(new OpenTSDBSchema(ts.getPhysicalPath(), storageUnit));
                        break;
                    }
                }
            }
        }
        return schemas;
    }

    private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
            case NonAlignedRow:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
            case NonAlignedColumn:
                e = insertColRecords((ColumnDataView) dataView, storageUnit);
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in opentsdb failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Exception insertRowRecords(RowDataView dataView, String storageUnit) {
        DataViewWrapper data = new DataViewWrapper(dataView);
        List<OpenTSDBSchema> schemas = new ArrayList<>();
        for (int i = 0; i < data.getPathNum(); i++) {
            schemas.add(new OpenTSDBSchema(data.getPath(i), storageUnit));
        }

        List<Point> points = new ArrayList<>();
        for (int i = 0; i < data.getTimeSize(); i++) {
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getPathNum(); j++) {
                if (bitmapView.get(j)) {
                    OpenTSDBSchema schema = schemas.get(j);
                    DataType type = data.getDataType(j);
                    switch (type) {
                        case BOOLEAN:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (boolean) data.getValue(i, index) ? 1 : 0).build());
                            break;
                        case INTEGER:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (int) data.getValue(i, index)).build());
                            break;
                        case LONG:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (long) data.getValue(i, index)).build());
                            break;
                        case FLOAT:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (float) data.getValue(i, index)).build());
                            break;
                        case DOUBLE:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (double) data.getValue(i, index)).build());
                            break;
                        case BINARY:
                            return new PhysicalTaskExecuteFailureException("opentsdb not support string for now!");
                    }
                    index++;
                }
            }
        }

        try {
            logger.info("开始数据写入");
            client.putSync(points);
        } catch (Exception e) {
            logger.error("encounter error when write points to opentsdb: ", e);
        } finally {
            logger.info("数据写入完毕！");
        }
        return null;
    }

    private Exception insertColRecords(ColumnDataView dataView, String storageUnit) {
        DataViewWrapper data = new DataViewWrapper(dataView);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < data.getPathNum(); i++) {
            OpenTSDBSchema schema = new OpenTSDBSchema(data.getPath(i), storageUnit);
            BitmapView bitmapView = data.getBitmapView(i);
            int index = 0;
            for (int j = 0; j < data.getTimeSize(); j++) {
                if (bitmapView.get(j)) {
                    DataType type = data.getDataType(i);
                    switch (type) {
                        case BOOLEAN:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (boolean) data.getValue(i, index) ? 1 : 0).build());
                            break;
                        case INTEGER:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (int) data.getValue(i, index)).build());
                            break;
                        case LONG:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (long) data.getValue(i, index)).build());
                            break;
                        case FLOAT:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (float) data.getValue(i, index)).build());
                            break;
                        case DOUBLE:
                            points.add(Point.metric(schema.getMetric()).tag(schema.getTags()).tag(DATA_TYPE, type.toString()).value(data.getTimestamp(i), (double) data.getValue(i, index)).build());
                            break;
                        case BINARY:
                            return new PhysicalTaskExecuteFailureException("opentsdb not support string for now!");
                    }
                    index++;
                }
            }
        }

        try {
            logger.info("开始数据写入");
            client.putSync(points);
        } catch (Exception e) {
            logger.error("encounter error when write points to opentsdb: ", e);
        } finally {
            logger.info("数据写入完毕！");
        }
        return null;
    }

    private TaskExecuteResult executeProjectTask(TimeInterval timeInterval, String storageUnit, Project project) {
        List<String> wholePathList;
        try {
            wholePathList = getPathList();
        } catch (PhysicalTaskExecuteFailureException e) {
            return new TaskExecuteResult(new PhysicalException("encounter error when query data in opentsdb: ", e));
        }

        Query.Builder builder = Query.begin(timeInterval.getStartTime()).end(timeInterval.getEndTime()).msResolution();
        for (String queryPath : project.getPatterns()) {
            if (StringUtils.isPattern(queryPath)) {
                Pattern pattern = Pattern.compile(StringUtils.reformatPath(queryPath));
                for (String path : wholePathList) {
                    if (pattern.matcher(path).matches()) {
                        OpenTSDBSchema schema = new OpenTSDBSchema(path, storageUnit);
                        builder = builder.sub(SubQuery.metric(schema.getMetric()).aggregator(SubQuery.Aggregator.NONE).build());
                    }
                }
            } else {
                OpenTSDBSchema schema = new OpenTSDBSchema(queryPath, storageUnit);
                builder = builder.sub(SubQuery.metric(schema.getMetric()).aggregator(SubQuery.Aggregator.NONE).build());
            }
        }
        Query query = builder.build();
        try {
            List<QueryResult> resultList = client.query(query);
            RowStream rowStream = new ClearEmptyRowStreamWrapper(new OpenTSDBRowStream(resultList, true));
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            return new TaskExecuteResult(new PhysicalException("encounter error when query data in opentsdb: ", e));
        }
    }

    private TaskExecuteResult executeProjectHistoryTask(TimeInterval timeInterval, String storageUnit, Project project) {
        List<String> wholePathList;
        try {
            wholePathList = getPathList();
        } catch (PhysicalTaskExecuteFailureException e) {
            return new TaskExecuteResult(new PhysicalException("encounter error when query data in opentsdb: ", e));
        }

        Query.Builder builder = Query.begin(timeInterval.getStartTime()).end(timeInterval.getEndTime()).msResolution();
        for (String queryPath : project.getPatterns()) {
            if (StringUtils.isPattern(queryPath)) {
                Pattern pattern = Pattern.compile(StringUtils.reformatPath(queryPath));
                for (String path : wholePathList) {
                    if (pattern.matcher(path).matches()) {
                        builder = builder.sub(SubQuery.metric(path).aggregator(SubQuery.Aggregator.NONE).build());
                    }
                }
            } else {
                builder = builder.sub(SubQuery.metric(queryPath).aggregator(SubQuery.Aggregator.NONE).build());
            }
        }
        Query query = builder.build();
        try {
            List<QueryResult> resultList = client.query(query);
            RowStream rowStream = new ClearEmptyRowStreamWrapper(new OpenTSDBRowStream(resultList, false));
            return new TaskExecuteResult(rowStream);
        } catch (Exception e) {
            return new TaskExecuteResult(new PhysicalException("encounter error when query data in opentsdb: ", e));
        }
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        List<String> paths = getPurePath();
        paths.sort(String::compareTo);
        if (paths.isEmpty()) {
            throw new PhysicalTaskExecuteFailureException("no data!");
        }
        TimeSeriesRange tsInterval = new TimeSeriesInterval(paths.get(0), StringUtils.nextString(paths.get(paths.size() - 1)));

        long minTime = 0, maxTime = Long.MAX_VALUE - 1;
        Query.Builder builder = Query.begin(0L).end(Long.MAX_VALUE).msResolution();
        for (String path : paths) {
            builder = builder.sub(SubQuery.metric(path).aggregator(SubQuery.Aggregator.NONE).build());
        }
        Query query = builder.build();
        try {
            List<QueryResult> resultList = client.query(query);
            List<Long> times = new ArrayList<>();
            for (QueryResult result : resultList) {
                times.addAll(result.getDps().keySet());
            }
            if (!times.isEmpty()) {
                times.sort(Long::compareTo);
                minTime = times.get(0);
                maxTime = times.get(times.size() - 1);
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when query data in opentsdb: ", e);
        }
        TimeInterval timeInterval = new TimeInterval(minTime, maxTime + 1);
        return new Pair<>(tsInterval, timeInterval);
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        List<Timeseries> timeseries = new ArrayList<>();

        List<String> paths = getPathWithoutDUPrefixList();
        if (paths.isEmpty()) {
            return timeseries;
        }

        Query.Builder builder = Query.begin(0L).end(Long.MAX_VALUE).msResolution();
        for (String path : paths) {
            builder = builder.sub(SubQuery.metric(path).aggregator(SubQuery.Aggregator.NONE).build());
        }
        Query query = builder.build();
        try {
            List<QueryResult> resultList = client.query(query);
            for (QueryResult result : resultList) {
                String path = result.getMetric();
                if (path.startsWith("unit")) {
                    path = path.substring(path.indexOf('.') + 1);
                }
                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
                timeseries.add(new Timeseries(pair.k, fromOpenTSDB(result.getTags().get(DATA_TYPE)), pair.v));
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when query data in opentsdb: ", e);
        }
        return timeseries;
    }

    private List<String> getPurePath() throws PhysicalTaskExecuteFailureException {
        List<String> suggests = getPathList();

        List<String> paths = new ArrayList<>();
        for (String metric : suggests) {
            String path = metric;
            if (path.startsWith(DU_PREFIX)) {
                path = metric.substring(metric.indexOf(".") + 1);
            }
            paths.add(TagKVUtils.splitFullName(path).k);
        }
        return paths;
    }

    private List<String> getPathWithoutDUPrefixList() throws PhysicalTaskExecuteFailureException {
        List<String> suggests = getPathList();

        List<String> paths = new ArrayList<>();
        for (String metric : suggests) {
            if (metric.startsWith(DU_PREFIX)) {
                paths.add(metric.substring(metric.indexOf(".") + 1));
            } else {
                paths.add(metric);
            }
        }
        return paths;
    }

    private List<String> getPathList() throws PhysicalTaskExecuteFailureException {
        SuggestQuery suggestQuery = SuggestQuery.type(SuggestQuery.Type.METRICS).max(Integer.MAX_VALUE).build();
        try {
            return client.querySuggest(suggestQuery);
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("get time series failure: ", e);
        }
    }

    @Override
    public void release() throws PhysicalException {
        try {
            client.gracefulClose();
        } catch (IOException e) {
            logger.error("can not close opentsdb gracefully, because " + e.getMessage());
            try {
                client.forceClose();
            } catch (IOException ioException) {
                throw new PhysicalTaskExecuteFailureException("can not close opentsdb, because " + e.getMessage());
            }
        }
    }
}
