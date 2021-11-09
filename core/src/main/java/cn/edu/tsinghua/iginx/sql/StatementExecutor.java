package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.sql.logical.LogicalGenerator;
import cn.edu.tsinghua.iginx.sql.logical.QueryGenerator;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementBuilder;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class StatementExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StatementExecutor.class);

    private final static StatementExecutor instance = new StatementExecutor();

    private final static StatementBuilder builder = StatementBuilder.getInstance();

    private final static PhysicalEngine engine = PhysicalEngineImpl.getInstance();

    private final static ConstraintManager constraintManager = engine.getConstraintManager();

    private final List<LogicalGenerator> generatorList = new ArrayList<>();

    private final static Config config = ConfigDescriptor.getInstance().getConfig();

    private StatementExecutor() {
        registerGenerator(QueryGenerator.getInstance());
    }

    public static StatementExecutor getInstance() {
        return instance;
    }

    public void registerGenerator(LogicalGenerator generator) {
        if (generator != null)
            generatorList.add(generator);
    }

    public ExecuteSqlResp execute(String sql, long sessionId) {
        try {
            Statement statement = builder.build(sql);
            if (statement.getType() == Statement.StatementType.SELECT && config.isQueryInNewWay()) {
                return processQuery((SelectStatement) statement);
            } else {
                return statement.execute(sessionId);
            }
        } catch (SQLParserException | ParseCancellationException e) {
            StatusCode statusCode = StatusCode.STATEMENT_PARSE_ERROR;
            return buildErrResp(statusCode, e.getMessage());
        } catch (ExecutionException | PhysicalException e) {
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            return buildErrResp(statusCode, e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            StatusCode statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
            String errMsg = "Execute Error: encounter error(s) when executing sql statement, " +
                    "see server log for more details.";
            return buildErrResp(statusCode, errMsg);
        }
    }

    private ExecuteSqlResp processQuery(SelectStatement statement) throws ExecutionException, PhysicalException {
        for (LogicalGenerator generator: generatorList) {
            Operator root = generator.generate(statement);
            if (constraintManager.check(root)) {
                RowStream stream = engine.execute(root);
                return buildRowStreamResp(stream, statement);
            }
        }
        throw new ExecutionException("Execute Error: can not construct a legal logical tree.");
    }

    private ExecuteSqlResp buildErrResp(StatusCode statusCode, String errMsg) {
        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.status(statusCode, errMsg), SqlType.Unknown);
        resp.setParseErrorMsg(errMsg);
        return resp;
    }

    private ExecuteSqlResp buildRowStreamResp(RowStream stream, SelectStatement statement) throws PhysicalException {
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
                timestampList.add(row.getTimestamp());
            }
        }

        logger.debug("selected paths num: {}", paths.size());
        logger.debug("selected types num: {}", types.size());
        logger.debug("time stamp num: {}", timestampList.size());
        logger.debug("value row num: {}", valuesList.size());

        ExecuteSqlResp resp;
        if (!timestampList.isEmpty()) {
            resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.SimpleQuery);
            resp.setPaths(paths);
            resp.setDataTypeList(types);
            resp.setOffset(0);
            resp.setLimit(Integer.MAX_VALUE);

            Long[] timestamps = timestampList.toArray(new Long[timestampList.size()]);
            ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
            resp.setTimestamps(timeBuffer);

            QueryDataSet set = new QueryDataSet(timeBuffer, valuesList, bitmapList);
            resp.setQueryDataSet(set);
        } else {
            resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.AggregateQuery);
            resp.setPaths(paths);
            resp.setDataTypeList(types);
            if (!valuesList.isEmpty()) {
                resp.setValuesList(valuesList.get(0));
            }
            resp.setOffset(0);
            resp.setLimit(Integer.MAX_VALUE);
        }
        return resp;
    }
}
