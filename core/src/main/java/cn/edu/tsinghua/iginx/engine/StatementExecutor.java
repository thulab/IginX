package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintChecker;
import cn.edu.tsinghua.iginx.engine.logical.constraint.ConstraintCheckerManager;
import cn.edu.tsinghua.iginx.engine.logical.generator.*;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.processor.*;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.resource.ResourceManager;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.statistics.IStatisticsCollector;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

public class StatementExecutor {

    private final static Logger logger = LoggerFactory.getLogger(StatementExecutor.class);

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private final static StatementBuilder builder = StatementBuilder.getInstance();

    private final static PhysicalEngine engine = PhysicalEngineImpl.getInstance();

    private final static ConstraintChecker checker = ConstraintCheckerManager.getInstance().getChecker(config.getConstraintChecker());
    private final static ConstraintManager constraintManager = engine.getConstraintManager();

    private final static ResourceManager resourceManager = ResourceManager.getInstance();

    private final static Map<StatementType, List<LogicalGenerator>> generatorMap = new HashMap<>();

    private final static List<LogicalGenerator> queryGeneratorList = new ArrayList<>();
    private final static List<LogicalGenerator> deleteGeneratorList = new ArrayList<>();
    private final static List<LogicalGenerator> insertGeneratorList = new ArrayList<>();
    private final static List<LogicalGenerator> showTSGeneratorList = new ArrayList<>();

    private final List<PreParseProcessor> preParseProcessors = new ArrayList<>();
    private final List<PostParseProcessor> postParseProcessors = new ArrayList<>();
    private final List<PreLogicalProcessor> preLogicalProcessors = new ArrayList<>();
    private final List<PostLogicalProcessor> postLogicalProcessors = new ArrayList<>();
    private final List<PrePhysicalProcessor> prePhysicalProcessors = new ArrayList<>();
    private final List<PostPhysicalProcessor> postPhysicalProcessors = new ArrayList<>();
    private final List<PreExecuteProcessor> preExecuteProcessors = new ArrayList<>();
    private final List<PostExecuteProcessor> postExecuteProcessors = new ArrayList<>();

    private static class StatementExecutorHolder {
        private final static StatementExecutor instance = new StatementExecutor();
    }

    static {
        generatorMap.put(StatementType.SELECT, queryGeneratorList);
        generatorMap.put(StatementType.DELETE, deleteGeneratorList);
        generatorMap.put(StatementType.INSERT, insertGeneratorList);
        generatorMap.put(StatementType.SHOW_TIME_SERIES, showTSGeneratorList);
    }

    private StatementExecutor() {
        registerGenerator(QueryGenerator.getInstance());
        registerGenerator(DeleteGenerator.getInstance());
        registerGenerator(InsertGenerator.getInstance());
        registerGenerator(ShowTimeSeriesGenerator.getInstance());

        try {
            String statisticsCollectorClassName = ConfigDescriptor.getInstance().getConfig().getStatisticsCollectorClassName();
            if (statisticsCollectorClassName != null && !statisticsCollectorClassName.equals("")) {
                Class<?> statisticsCollectorClass = StatementExecutor.class.getClassLoader().
                    loadClass(statisticsCollectorClassName);
                IStatisticsCollector statisticsCollector = ((Class<? extends IStatisticsCollector>) statisticsCollectorClass)
                    .getConstructor().newInstance();
                registerPreParseProcessor(statisticsCollector.getPreParseProcessor());
                registerPostParseProcessor(statisticsCollector.getPostParseProcessor());
                registerPreLogicalProcessor(statisticsCollector.getPreLogicalProcessor());
                registerPostLogicalProcessor(statisticsCollector.getPostLogicalProcessor());
                registerPrePhysicalProcessor(statisticsCollector.getPrePhysicalProcessor());
                registerPostPhysicalProcessor(statisticsCollector.getPostPhysicalProcessor());
                registerPreExecuteProcessor(statisticsCollector.getPreExecuteProcessor());
                registerPostExecuteProcessor(statisticsCollector.getPostExecuteProcessor());
                statisticsCollector.startBroadcasting();
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            logger.error("initial statistics collector error: ", e);
        }
    }

    public static StatementExecutor getInstance() {
        return StatementExecutorHolder.instance;
    }

    public void registerGenerator(LogicalGenerator generator) {
        if (generator != null) {
            switch (generator.getType()) {
                case Query:
                    queryGeneratorList.add(generator);
                    break;
                case Delete:
                    deleteGeneratorList.add(generator);
                    break;
                case Insert:
                    insertGeneratorList.add(generator);
                    break;
                case ShowTimeSeries:
                    showTSGeneratorList.add(generator);
                    break;
                default:
                    throw new IllegalArgumentException("unknown generator type");
            }
        }
    }

    public void registerPreParseProcessor(PreParseProcessor processor) {
        if (processor != null) preParseProcessors.add(processor);
    }

    public void registerPostParseProcessor(PostParseProcessor processor) {
        if (processor != null) postParseProcessors.add(processor);
    }

    public void registerPreLogicalProcessor(PreLogicalProcessor processor) {
        if (processor != null) preLogicalProcessors.add(processor);
    }

    public void registerPostLogicalProcessor(PostLogicalProcessor processor) {
        if (processor != null) postLogicalProcessors.add(processor);
    }

    public void registerPrePhysicalProcessor(PrePhysicalProcessor processor) {
        if (processor != null) prePhysicalProcessors.add(processor);
    }

    public void registerPostPhysicalProcessor(PostPhysicalProcessor processor) {
        if (processor != null) postPhysicalProcessors.add(processor);
    }

    public void registerPreExecuteProcessor(PreExecuteProcessor processor) {
        if (processor != null) preExecuteProcessors.add(processor);
    }

    public void registerPostExecuteProcessor(PostExecuteProcessor processor) {
        if (processor != null) postExecuteProcessors.add(processor);
    }

    public void execute(RequestContext ctx) {
        if (config.isEnableMemoryControl() && resourceManager.reject(ctx)) {
            ctx.setResult(new Result(RpcUtils.SERVICE_UNAVAILABLE));
            return;
        }
        before(ctx, preExecuteProcessors);
        if (ctx.isFromSQL()) {
            executeSQL(ctx);
        } else {
            executeStatement(ctx);
        }
        after(ctx, postExecuteProcessors);
    }

    public void executeSQL(RequestContext ctx) {
        try {
            before(ctx, preParseProcessors);
            builder.buildFromSQL(ctx);
            after(ctx, postParseProcessors);
            executeStatement(ctx);
        } catch (SQLParserException | ParseCancellationException e) {
            StatusCode statusCode = StatusCode.STATEMENT_PARSE_ERROR;
            ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
        } catch (Exception e) {
            e.printStackTrace();
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            String errMsg = "Execute Error: encounter error(s) when executing sql statement, " +
                "see server log for more details.";
            ctx.setResult(new Result(RpcUtils.status(statusCode, errMsg)));
        } finally {
            ctx.getResult().setSqlType(ctx.getSqlType());
        }
    }

    public void executeStatement(RequestContext ctx) {
        try {
            Statement statement = ctx.getStatement();
            if (statement instanceof DataStatement) {
                StatementType type = statement.getType();
                switch (type) {
                    case SELECT:
                    case DELETE:
                    case INSERT:
                    case SHOW_TIME_SERIES:
                        process(ctx);
                        return;
                    case INSERT_FROM_SELECT:
                        processInsertFromSelect(ctx);
                        return;
                    case COUNT_POINTS:
                        processCountPoints(ctx);
                        return;
                    case DELETE_TIME_SERIES:
                        processDeleteTimeSeries(ctx);
                        return;
                    case CLEAR_DATA:
                        processClearData(ctx);
                        return;
                    default:
                        throw new ExecutionException(String.format("Execute Error: unknown statement type [%s].", type));
                }
            } else {
                ((SystemStatement) statement).execute(ctx);
            }
        } catch (ExecutionException | PhysicalException e) {
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
        } catch (Exception e) {
            logger.error("unexpected exception during dispatcher memory task, please contact developer to check: ", e);
            StatusCode statusCode = StatusCode.SYSTEM_ERROR;
            ctx.setResult(new Result(RpcUtils.status(statusCode, e.getMessage())));
        }
    }

    private void process(RequestContext ctx) throws ExecutionException, PhysicalException {
        List<LogicalGenerator> generatorList = generatorMap.get(ctx.getStatement().getType());
        for (LogicalGenerator generator : generatorList) {
            before(ctx, preLogicalProcessors);
            Operator root = generator.generate(ctx);
            after(ctx, postLogicalProcessors);
            if (constraintManager.check(root) && checker.check(root)) {
                before(ctx, prePhysicalProcessors);
                RowStream stream = engine.execute(root);
                after(ctx, postPhysicalProcessors);
                setResult(ctx, stream);
                return;
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private void processInsertFromSelect(RequestContext ctx) throws ExecutionException, PhysicalException {
        InsertFromSelectStatement statement = (InsertFromSelectStatement) ctx.getStatement();

        // step 1: select stage
        SelectStatement selectStatement = statement.getSubSelectStatement();
        RequestContext subSelectContext = new RequestContext(ctx.getSessionId(), selectStatement, true);
        process(subSelectContext);

        RowStream rowStream = subSelectContext.getResult().getResultStream();

        // step 2: insert stage
        InsertStatement insertStatement = statement.getSubInsertStatement();
        parseOldTagsFromHeader(rowStream.getHeader(), insertStatement);
        parseInsertValuesSpecFromRowStream(statement.getTimeOffset(), rowStream, insertStatement);
        RequestContext subInsertContext = new RequestContext(ctx.getSessionId(), insertStatement, ctx.isUseStream());
        process(subInsertContext);

        ctx.setResult(subInsertContext.getResult());
    }

    private void processCountPoints(RequestContext ctx) throws ExecutionException, PhysicalException {
        SelectStatement statement = new SelectStatement(
            Collections.singletonList("*"),
            0,
            Long.MAX_VALUE,
            AggregateType.COUNT);
        ctx.setStatement(statement);
        process(ctx);

        Result result = ctx.getResult();
        long pointsNum = 0;
        if (ctx.getResult().getValuesList() != null) {
            Object[] row = ByteUtils.getValuesByDataType(result.getValuesList().get(0), result.getDataTypes());
            pointsNum = Arrays.stream(row).mapToLong(e -> (Long) e).sum();
        }

        ctx.getResult().setPointsNum(pointsNum);
    }

    private void processDeleteTimeSeries(RequestContext ctx) throws ExecutionException, PhysicalException {
        DeleteTimeSeriesStatement deleteTimeSeriesStatement = (DeleteTimeSeriesStatement) ctx.getStatement();
        DeleteStatement deleteStatement = new DeleteStatement(
            deleteTimeSeriesStatement.getPaths(),
            deleteTimeSeriesStatement.getTagFilter()
        );
        ctx.setStatement(deleteStatement);
        process(ctx);
    }

    private void processClearData(RequestContext ctx) throws ExecutionException, PhysicalException {
        DeleteStatement deleteStatement = new DeleteStatement(Collections.singletonList("*"));
        ctx.setStatement(deleteStatement);
        process(ctx);
    }

    private void setEmptyQueryResp(RequestContext ctx) {
        Result result = new Result(RpcUtils.SUCCESS);
        result.setTimestamps(new Long[0]);
        result.setValuesList(new ArrayList<>());
        result.setBitmapList(new ArrayList<>());
        result.setPaths(new ArrayList<>());
        ctx.setResult(result);
    }

    private void setResult(RequestContext ctx, RowStream stream) throws PhysicalException, ExecutionException {
        Statement statement = ctx.getStatement();
        switch (statement.getType()) {
            case INSERT:
                ctx.setResult(new Result(RpcUtils.SUCCESS));
                break;
            case DELETE:
                DeleteStatement deleteStatement = (DeleteStatement) statement;
                if (deleteStatement.isInvolveDummyData()) {
                    throw new ExecutionException("Caution: can not clear the data of read-only node.");
                } else {
                    ctx.setResult(new Result(RpcUtils.SUCCESS));
                }
                break;
            case SELECT:
                setResultFromRowStream(ctx, stream);
                break;
            case SHOW_TIME_SERIES:
                setShowTSRowStreamResult(ctx, stream);
                break;
            default:
                throw new ExecutionException(String.format("Execute Error: unknown statement type [%s].", statement.getType()));
        }
    }

    private void setResultFromRowStream(RequestContext ctx, RowStream stream) throws PhysicalException {
        if (ctx.isUseStream()) {
            Result result = new Result(RpcUtils.SUCCESS);
            result.setResultStream(stream);
            ctx.setResult(result);
            return;
        }
        List<String> paths = new ArrayList<>();
        List<Map<String, String>> tagsList = new ArrayList<>();
        List<DataType> types = new ArrayList<>();
        stream.getHeader().getFields().forEach(field -> {
            paths.add(field.getFullName());
            types.add(field.getType());
            if (field.getTags() == null) {
                tagsList.add(new HashMap<>());
            } else {
                tagsList.add(field.getTags());
            }
        });

        List<Long> timestampList = new ArrayList<>();
        List<ByteBuffer> valuesList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();

        boolean hasTimestamp = stream.getHeader().hasKey();
        while (stream.hasNext()) {
            Row row = stream.next();

            Object[] rowValues = row.getValues();
            valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));

            Bitmap bitmap = new Bitmap(rowValues.length);
            for (int i = 0; i < rowValues.length; i++) {
                if (rowValues[i] != null) {
                    bitmap.mark(i);
                }
            }
            bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));

            if (hasTimestamp) {
                timestampList.add(row.getKey());
            }
        }

        if (valuesList.isEmpty()) { // empty result
            setEmptyQueryResp(ctx);
            return;
        }

        Result result = new Result(RpcUtils.SUCCESS);
        if (timestampList.size() != 0) {
            Long[] timestamps = timestampList.toArray(new Long[timestampList.size()]);
            result.setTimestamps(timestamps);
        }
        result.setValuesList(valuesList);
        result.setBitmapList(bitmapList);
        result.setPaths(paths);
        result.setTagsList(tagsList);
        result.setDataTypes(types);
        ctx.setResult(result);

        stream.close();
    }

    private void setShowTSRowStreamResult(RequestContext ctx, RowStream stream) throws PhysicalException {
        if (ctx.isUseStream()) {
            Result result = new Result(RpcUtils.SUCCESS);
            result.setResultStream(stream);
            ctx.setResult(result);
            return;
        }
        List<String> paths = new ArrayList<>();
        // todo:need physical layer to support.
        List<Map<String, String>> tagsList = new ArrayList<>();
        List<DataType> types = new ArrayList<>();

        while (stream.hasNext()) {
            Row row = stream.next();
            Object[] rowValues = row.getValues();

            if (rowValues.length == 2) {
                paths.add(new String((byte[]) rowValues[0]));
                DataType type = DataTypeUtils.strToDataType(new String((byte[]) rowValues[1]));
                if (type == null) {
                    logger.warn("unknown data type [{}]", rowValues[1]);
                }
                types.add(type);
            } else {
                logger.warn("show time series result col size = {}", rowValues.length);
            }
        }

        Result result = new Result(RpcUtils.SUCCESS);
        result.setPaths(paths);
        result.setTagsList(tagsList);
        result.setDataTypes(types);
        ctx.setResult(result);
    }

    private void parseOldTagsFromHeader(Header header, InsertStatement insertStatement) throws PhysicalException, ExecutionException {
        if (insertStatement.getPaths().size() != header.getFieldSize()) {
            throw new ExecutionException("Execute Error: Insert path size and value size must be equal.");
        }
        List<Field> fields = header.getFields();
        List<Map<String, String>> tagsList = insertStatement.getTagsList();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Map<String, String> tags = tagsList.get(i);
            Map<String, String> oldTags = field.getTags();
            if (oldTags != null && !oldTags.isEmpty()) {
                if (tags == null) {
                    tagsList.set(i, oldTags);
                } else {
                    tags.putAll(oldTags);
                }
            }
        }
    }

    private void parseInsertValuesSpecFromRowStream(long offset, RowStream rowStream, InsertStatement insertStatement) throws PhysicalException, ExecutionException {
        Header header = rowStream.getHeader();
        if (insertStatement.getPaths().size() != header.getFieldSize()) {
            throw new ExecutionException("Execute Error: Insert path size and value size must be equal.");
        }

        List<DataType> types = new ArrayList<>();
        header.getFields().forEach(field -> types.add(field.getType()));

        List<Long> times = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();
        List<Bitmap> bitmaps = new ArrayList<>();

        for (long i = 0; rowStream.hasNext(); i++) {
            Row row = rowStream.next();
            rows.add(row.getValues());

            int rowLen = row.getValues().length;
            Bitmap bitmap = new Bitmap(rowLen);
            for (int j = 0; j < rowLen; j++) {
                if (row.getValue(j) != null) {
                    bitmap.mark(j);
                }
            }
            bitmaps.add(bitmap);

            if (header.hasKey()) {
                times.add(row.getKey() + offset);
            } else {
                times.add(i + offset);
            }
        }
        Object[][] values = rows.toArray(new Object[0][0]);

        insertStatement.setTimes(times);
        insertStatement.setValues(values);
        insertStatement.setTypes(types);
        insertStatement.setBitmaps(bitmaps);
    }

    private void before(RequestContext ctx, List<? extends Processor> list) {
        record(ctx, list);
    }

    private void after(RequestContext ctx, List<? extends Processor> list) {
        record(ctx, list);
    }

    private void record(RequestContext ctx, List<? extends Processor> list) {
        for (Processor processor : list) {
            Status status = processor.process(ctx);
            if (status != null) {
                ctx.setStatus(status);
                return;
            }
        }
    }
}
