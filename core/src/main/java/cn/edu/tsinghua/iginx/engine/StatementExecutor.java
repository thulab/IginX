package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.generator.DeleteGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.InsertGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.LogicalGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.QueryGenerator;
import cn.edu.tsinghua.iginx.engine.logical.generator.ShowTimeSeriesGenerator;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteTimeSeriesStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowTimeSeriesStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.SystemStatement;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StatementExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StatementExecutor.class);

    private final static StatementExecutor instance = new StatementExecutor();

    private final static StatementBuilder builder = StatementBuilder.getInstance();

    private final static PhysicalEngine engine = PhysicalEngineImpl.getInstance();

    private final static ConstraintManager constraintManager = engine.getConstraintManager();
    private final static Config config = ConfigDescriptor.getInstance().getConfig();
    private final List<LogicalGenerator> queryGeneratorList = new ArrayList<>();
    private final List<LogicalGenerator> deleteGeneratorList = new ArrayList<>();
    private final List<LogicalGenerator> insertGeneratorList = new ArrayList<>();
    private final List<LogicalGenerator> showTSGeneratorList = new ArrayList<>();

    private StatementExecutor() {
        registerGenerator(QueryGenerator.getInstance());
        registerGenerator(DeleteGenerator.getInstance());
        registerGenerator(InsertGenerator.getInstance());
        registerGenerator(ShowTimeSeriesGenerator.getInstance());
    }

    public static StatementExecutor getInstance() {
        return instance;
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

    public ExecuteSqlResp execute(String sql, long sessionId) {
        try {
            Statement statement = builder.build(sql);
            return executeStatement(statement, sessionId);
        } catch (SQLParserException | ParseCancellationException e) {
            StatusCode statusCode = StatusCode.STATEMENT_PARSE_ERROR;
            return buildErrResp(statusCode, e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            String errMsg = "Execute Error: encounter error(s) when executing sql statement, " +
                    "see server log for more details.";
            return buildErrResp(statusCode, errMsg);
        }
    }

    public ExecuteSqlResp executeStatement(Statement statement, long sessionId) {
        try {
            StatementType type = statement.getType();
            switch (type) {
                case SELECT:
                    return processQuery((SelectStatement) statement);
                case DELETE:
                    return processDelete((DeleteStatement) statement);
                case INSERT:
                    return processInsert((InsertStatement) statement);
                case SHOW_TIME_SERIES:
                    return processShowTimeSeries((ShowTimeSeriesStatement) statement);
                case COUNT_POINTS:
                    return processCountPoints();
                case DELETE_TIME_SERIES:
                    return processDeleteTimeSeries((DeleteTimeSeriesStatement) statement);
                case CLEAR_DATA:
                    return processClearData();
                default:
                    return ((SystemStatement) statement).execute(sessionId);
            }
        } catch (ExecutionException | PhysicalException e) {
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            return buildErrResp(statusCode, e.getMessage());
        }
    }

    private ExecuteSqlResp processQuery(SelectStatement statement) throws ExecutionException, PhysicalException {
        for (LogicalGenerator generator : queryGeneratorList) {
            Operator root = generator.generate(statement);
            if (constraintManager.check(root)) {
                RowStream stream = engine.execute(root);
                return buildQueryRowStreamResp(stream, statement);
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private ExecuteSqlResp processDelete(DeleteStatement statement) throws ExecutionException, PhysicalException {
        for (LogicalGenerator generator : deleteGeneratorList) {
            Operator root = generator.generate(statement);
            if (constraintManager.check(root)) {
                engine.execute(root);
                return new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.Delete);
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private ExecuteSqlResp processInsert(InsertStatement statement) throws ExecutionException, PhysicalException {
        for (LogicalGenerator generator : insertGeneratorList) {
            Operator root = generator.generate(statement);
            if (constraintManager.check(root)) {
                engine.execute(root);
                return new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.Insert);
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private ExecuteSqlResp processShowTimeSeries(ShowTimeSeriesStatement statement) throws ExecutionException, PhysicalException {
        for (LogicalGenerator generator : showTSGeneratorList) {
            Operator root = generator.generate(statement);
            if (constraintManager.check(root)) {
                RowStream stream = engine.execute(root);
                return buildShowTSRowStreamResp(stream);
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private ExecuteSqlResp processCountPoints() throws ExecutionException, PhysicalException {
        SelectStatement statement = new SelectStatement(
                Collections.singletonList("*"),
                0,
                Long.MAX_VALUE,
                AggregateType.COUNT);
        ExecuteSqlResp countResp = processQuery(statement);
        long pointsNum = 0;
        if (countResp.getValuesList() != null) {
            Object[] row = ByteUtils.getValuesByDataType(countResp.valuesList, countResp.dataTypeList);
            for (Object count : row) {
                pointsNum += (Long) count;
            }
        }
        ExecuteSqlResp resp = new ExecuteSqlResp(countResp.getStatus(), SqlType.CountPoints);
        resp.setPointsNum(pointsNum);
        return resp;
    }

    private ExecuteSqlResp processDeleteTimeSeries(DeleteTimeSeriesStatement statement) throws ExecutionException, PhysicalException {
        DeleteStatement deleteStatement = new DeleteStatement(statement.getPaths());
        return processDelete(deleteStatement);
    }

    private ExecuteSqlResp processClearData() throws ExecutionException, PhysicalException {
        DeleteStatement deleteStatement = new DeleteStatement(Collections.singletonList("*"));
        return processDelete(deleteStatement);
    }

    private ExecuteSqlResp buildErrResp(StatusCode statusCode, String errMsg) {
        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.status(statusCode, errMsg), SqlType.Unknown);
        resp.setParseErrorMsg(errMsg);
        return resp;
    }

    private ExecuteSqlResp buildEmptyQueryResp() {
        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.SimpleQuery);
        resp.setValuesList(new byte[]{});
        resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), new ArrayList<>(), new ArrayList<>()));
        return resp;
    }

    private ExecuteSqlResp buildQueryRowStreamResp(RowStream stream, SelectStatement statement) throws PhysicalException {
        List<String> paths = new ArrayList<>();
        List<DataType> types = new ArrayList<>();
        stream.getHeader().getFields().forEach(field -> {
            paths.add(field.getName());
            types.add(field.getType());
        });

        List<Long> timestampList = new ArrayList<>();
        List<ByteBuffer> valuesList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();

        boolean hasTimestamp = stream.getHeader().hasTimestamp();
        while(stream.hasNext()) {
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
                timestampList.add(row.getTimestamp());
            }
        }

        logger.debug("selected paths num: {}", paths.size());
        logger.debug("selected types num: {}", types.size());
        logger.debug("time stamp num: {}", timestampList.size());
        logger.debug("value row num: {}", valuesList.size());

        ExecuteSqlResp resp;
        if (!valuesList.isEmpty()) {
            if (!timestampList.isEmpty()) {
                resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.SimpleQuery);

                Long[] timestamps = timestampList.toArray(new Long[timestampList.size()]);
                ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
                resp.setTimestamps(timeBuffer);

                QueryDataSet set = new QueryDataSet(timeBuffer, valuesList, bitmapList);
                resp.setQueryDataSet(set);
            } else {
                resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.AggregateQuery);
                resp.setValuesList(valuesList.get(0));
            }
        } else {  // empty result
            resp = buildEmptyQueryResp();
        }

        resp.setPaths(paths);
        resp.setDataTypeList(types);
        resp.setOffset(0);
        resp.setLimit(Integer.MAX_VALUE);
        return resp;
    }

    private ExecuteSqlResp buildShowTSRowStreamResp(RowStream stream) throws PhysicalException {
        List<String> paths = new ArrayList<>();
        List<DataType> types = new ArrayList<>();

        while(stream.hasNext()) {
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

        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.ShowTimeSeries);
        resp.setPaths(paths);
        resp.setDataTypeList(types);
        return resp;
    }
}
