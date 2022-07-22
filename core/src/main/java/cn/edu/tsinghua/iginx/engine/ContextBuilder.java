package cn.edu.tsinghua.iginx.engine;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
            req.getTimestamps(), req.getValuesList(), req.getBitmapList(), req.getTagsList());
    }

    public RequestContext build(InsertNonAlignedColumnRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.NonAlignedColumn, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList(), req.getTagsList());
    }

    public RequestContext build(InsertRowRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.Row, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList(), req.getTagsList());
    }

    public RequestContext build(InsertNonAlignedRowRecordsReq req) {
        return buildFromInsertReq(req.getSessionId(), RawDataType.NonAlignedRow, req.getPaths(), req.getDataTypeList(),
            req.getTimestamps(), req.getValuesList(), req.getBitmapList(), req.getTagsList());
    }

    private RequestContext buildFromInsertReq(long sessionId, RawDataType rawDataType, List<String> paths, List<DataType> types,
                                              byte[] timestamps, List<ByteBuffer> valueList, List<ByteBuffer> bitmapList,
                                              List<Map<String, String>> tagsList) {
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
            bitmaps,
            tagsList
        );
        return new RequestContext(sessionId, statement);
    }

    public RequestContext build(DeleteDataInColumnsReq req) {
        DeleteStatement statement = new DeleteStatement(req.getPaths(), req.getStartTime(), req.getEndTime());
        // if (req.isSetTagsList()) {
        //     statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList()));
        // }
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(QueryDataReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime());
        if (req.isSetTagsList()) {
            statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList()));
        }
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(AggregateQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime(),
            req.getAggregateType());

        if (req.isSetTagsList()) {
            statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList()));
        }
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(DownsampleQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            req.getEndTime(),
            req.getAggregateType(),
            req.getPrecision());

        if (req.isSetTagsList()) {
            statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList()));
        }
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(ShowColumnsReq req) {
        ShowTimeSeriesStatement statement = new ShowTimeSeriesStatement();
        return new RequestContext(req.getSessionId(), statement);
    }

    public RequestContext build(ExecuteSqlReq req) {
        return new RequestContext(req.getSessionId(), req.getStatement());
    }

    public RequestContext build(ExecuteStatementReq req) {
        return new RequestContext(req.getSessionId(), req.getStatement(), true);
    }

    public RequestContext build(LastQueryReq req) {
        SelectStatement statement = new SelectStatement(
            req.getPaths(),
            req.getStartTime(),
            Long.MAX_VALUE,
            AggregateType.LAST);

        if (req.isSetTagsList()) {
            statement.setTagFilter(constructTagFilterFromTagList(req.getTagsList()));
        }
        return new RequestContext(req.getSessionId(), statement);
    }

    private TagFilter constructTagFilterFromTagList(Map<String, List<String>> tagList) {
        List<TagFilter> andTagFilterList = new ArrayList<>();
        tagList.forEach((key, valueList) -> {
            List<TagFilter> orTagFilterList = new ArrayList<>();
            valueList.forEach(value -> orTagFilterList.add(new BaseTagFilter(key, value)));
            andTagFilterList.add(new OrTagFilter(orTagFilterList));
        });
        return new AndTagFilter(andTagFilterList);
    }
}
