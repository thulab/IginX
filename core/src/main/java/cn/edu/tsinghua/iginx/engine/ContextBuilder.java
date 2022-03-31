package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteTimeSeriesStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ContextBuilder {

    private static ContextBuilder instance;

    private ContextBuilder() {
    }

    public static ContextBuilder getInstance() {
        if (instance == null) {
            synchronized (ContextBuilder.class) {
                if (instance == null) {
                    instance = new ContextBuilder();
                }
            }
        }
        return instance;
    }

    public RequestContext build(DeleteColumnsReq req) {
        DeleteTimeSeriesStatement statement = new DeleteTimeSeriesStatement(req.getPaths());
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(InsertColumnRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.Column, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList());
    }

    public RequestContext build(InsertNonAlignedColumnRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.NonAlignedColumn, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList());
    }

    public RequestContext build(InsertRowRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.Row, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList());
    }

    public RequestContext build(InsertNonAlignedRowRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.NonAlignedRow, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList());
    }

    private RequestContext buildFromInsertReq(long sessionId, RawDataType rawDataType, List<String> paths, List<DataType> types,
                                              byte[] timestamps, List<ByteBuffer> valueList, List<ByteBuffer> bitmapList) {
        long[] timeArray = ByteUtils.getLongArrayFromByteArray(timestamps);
        List<Long> times = new ArrayList<>();
        Arrays.stream(timeArray).forEach(times::add);

        List<Bitmap> bitmaps;
        Object[] values;
        if (rawDataType == RawDataType.Row || rawDataType == RawDataType.NonAlignedRow) {
            bitmaps = bitmapList.stream().map(x -> new Bitmap(paths.size(), x.array())).collect(Collectors.toList());
            values = ByteUtils.getRowValuesByDataType(valueList, types, bitmapList);
        } else {
            bitmaps = bitmapList.stream().map(x -> new Bitmap(times.size(), x.array())).collect(Collectors.toList());
            values = ByteUtils.getColumnValuesByDataType(valueList, types, bitmapList, times.size());
        }

        InsertStatement statement = new InsertStatement(
            rawDataType,
            paths,
            times,
            values,
            types,
            bitmaps
        );
        return new RequestContext(sessionId, statement);
    }

    public RequestContext build(DeleteDataInColumnsReq req) {
        DeleteStatement statement = new DeleteStatement(req.getPaths(), req.getStartTime(), req.getEndTime());
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(QueryDataReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime());
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(AggregateQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime(),
            req.getAggregateType());
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(DownsampleQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime(),
            req.getAggregateType(),
            req.getPrecision());
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(ShowColumnsReq req) {
        return new RequestContext(req.getSessionId());
    }

    public RequestContext build(ExecuteSqlReq req) {
        return new RequestContext(req.getSessionId(), req.getStatement());
    }

    public RequestContext build(ExecuteStatementReq req) {
        return null;
    }

    public RequestContext build(LastQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            Long.MAX_VALUE,
            AggregateType.LAST);
        return new RequestContext(req.getSessionId(), statement);
    }
}
