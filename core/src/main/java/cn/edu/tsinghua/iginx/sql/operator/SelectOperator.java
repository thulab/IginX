package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.combine.AggregateCombineResult;
import cn.edu.tsinghua.iginx.combine.DownsampleQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.LastQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.combine.ValueFilterCombineResult;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.LastQueryContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryReq;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectOperator extends Operator {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupBy;
    private boolean ascending;

    private List<Pair<String, String>> selectedFuncsAndPaths;
    private Set<FuncType> funcTypeSet;
    private String fromPath;
    private String orderByPath;
    private String booleanExpression;
    private long startTime;
    private long endTime;
    private long precision;
    private int limit;
    private int offset;

    public SelectOperator() {
        operatorType = OperatorType.SELECT;
        queryType = QueryType.Unknown;
        ascending = true;
        selectedFuncsAndPaths = new ArrayList<>();
        funcTypeSet = new HashSet<>();
        fromPath = "";
        orderByPath = "";
        startTime = Long.MIN_VALUE;
        endTime = Long.MAX_VALUE;
        limit = Integer.MAX_VALUE;
        offset = 0;
    }

    public static FuncType str2FuncType(String str) {
        switch (str.toLowerCase()) {
            case "first_value":
                return FuncType.FirstValue;
            case "last_value":
                return FuncType.LastValue;
            case "first":
                return FuncType.First;
            case "last":
                return FuncType.Last;
            case "min":
                return FuncType.Min;
            case "max":
                return FuncType.Max;
            case "avg":
                return FuncType.Avg;
            case "count":
                return FuncType.Count;
            case "sum":
                return FuncType.Sum;
            case "":
                return null;
            default:
                return FuncType.Udf;
        }
    }

    public static AggregateType funcType2AggregateType(FuncType type) {
        switch (type) {
            case First:
                return AggregateType.FIRST;
            case Last:
                return AggregateType.LAST;
            case FirstValue:
                return AggregateType.FIRST_VALUE;
            case LastValue:
                return AggregateType.LAST_VALUE;
            case Min:
                return AggregateType.MIN;
            case Max:
                return AggregateType.MAX;
            case Avg:
                return AggregateType.AVG;
            case Count:
                return AggregateType.COUNT;
            case Sum:
                return AggregateType.SUM;
            default:
                return null;
        }
    }

    public boolean isHasFunc() {
        return hasFunc;
    }

    public void setHasFunc(boolean hasFunc) {
        this.hasFunc = hasFunc;
    }

    public boolean isHasValueFilter() {
        return hasValueFilter;
    }

    public void setHasValueFilter(boolean hasValueFilter) {
        this.hasValueFilter = hasValueFilter;
    }

    public boolean isHasGroupBy() {
        return hasGroupBy;
    }

    public void setHasGroupBy(boolean hasGroupBy) {
        this.hasGroupBy = hasGroupBy;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public List<String> getSelectedPaths() {
        List<String> paths = new ArrayList<>();
        for (Pair<String, String> kv : selectedFuncsAndPaths) {
            paths.add(kv.v);
        }
        return paths;
    }

    public List<Pair<String, String>> getSelectedFuncsAndPaths() {
        return selectedFuncsAndPaths;
    }

    public void setSelectedFuncsAndPaths(String func, String path) {
        this.selectedFuncsAndPaths.add(new Pair<>(func, fromPath + SQLConstant.DOT + path));
        this.funcTypeSet.add(str2FuncType(func));
    }

    public Set<FuncType> getFuncTypeSet() {
        return funcTypeSet;
    }

    public void setFuncTypeSet(Set<FuncType> funcTypeSet) {
        this.funcTypeSet = funcTypeSet;
    }

    public String getFromPath() {
        return fromPath;
    }

    public void setFromPath(String path) {
        this.fromPath = path;
    }

    public String getOrderByPath() {
        return orderByPath;
    }

    public void setOrderByPath(String orderByPath) {

        this.orderByPath = orderByPath;
    }

    public String getBooleanExpression() {
        return booleanExpression;
    }

    public void setBooleanExpression(String booleanExpression) {
        this.booleanExpression = booleanExpression;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getPrecision() {
        return precision;
    }

    public void setPrecision(long precision) {
        this.precision = precision;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public void setQueryType() {
        if (hasFunc) {
            if (hasGroupBy && hasValueFilter) {
                this.queryType = QueryType.MixedQuery;
            } else if (hasGroupBy) {
                if (funcTypeSet.size() > 1) {
                    this.queryType = QueryType.MultiDownSampleQuery;
                } else {
                    this.queryType = QueryType.DownSampleQuery;
                }
            } else if (hasValueFilter) {
                this.queryType = QueryType.ValueFilterAndAggregateQuery;
            } else {
                if (funcTypeSet.size() > 1) {
                    this.queryType = QueryType.MultiAggregateQuery;
                } else {
                    this.queryType = QueryType.AggregateQuery;
                }
            }
        } else {
            if (hasGroupBy && hasValueFilter) {
                this.queryType = QueryType.ValueFilterAndDownSampleQuery;
            } else if (hasGroupBy) {
                throw new SQLParserException("Group by clause cannot be used without aggregate function.");
            } else if (hasValueFilter) {
                this.queryType = QueryType.ValueFilterQuery;
            } else {
                this.queryType = QueryType.SimpleQuery;
            }
        }
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        switch (queryType) {
            case SimpleQuery:
                return simpleQuery(sessionId);
            case AggregateQuery:
                return aggregateQuery(sessionId);
            case DownSampleQuery:
                return downSampleQuery(sessionId);
            case ValueFilterQuery:
                return valueFilterQuery(sessionId);
            default:
                ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.FAILURE, SqlType.NotSupportQuery);
                resp.setParseErrorMsg(String.format("Not support %s for now.", queryType.toString()));
                return resp;
        }
    }

    private ExecuteSqlResp simpleQuery(long sessionId) {
        Core core = Core.getInstance();
        QueryDataReq req = new QueryDataReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime
        );
        QueryDataContext ctx = new QueryDataContext(req);
        core.processRequest(ctx);
        QueryDataResp queryDataResp = ((QueryDataCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.SimpleQuery);
        resp.setPaths(queryDataResp.getPaths());
        resp.setDataTypeList(queryDataResp.getDataTypeList());
        resp.setQueryDataSet(queryDataResp.getQueryDataSet());
        resp.setLimit(limit);
        resp.setOffset(offset);
        resp.setOrderByPath(orderByPath);
        resp.setAscending(ascending);
        return resp;
    }

    private ExecuteSqlResp aggregateQuery(long sessionId) {
        String funcName = selectedFuncsAndPaths.get(0).k;
        FuncType funcType = str2FuncType(funcName);
        AggregateType aggregateType = funcType2AggregateType(funcType);

        if (aggregateType == null || aggregateType.equals(AggregateType.FIRST)) {
            throw new SQLParserException(String.format("Not support function %s in downSample query for now.", funcName));
        }
        if (aggregateType.equals(AggregateType.LAST)) {
            return lastAggregateQuery(sessionId);
        }

        Core core = Core.getInstance();
        AggregateQueryReq req = new AggregateQueryReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime,
                aggregateType
        );
        AggregateQueryContext ctx = new AggregateQueryContext(req);
        core.processRequest(ctx);
        AggregateQueryResp aggregateQueryResp = ((AggregateCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.AggregateQuery);
        resp.setPaths(aggregateQueryResp.getPaths());
        resp.setDataTypeList(aggregateQueryResp.getDataTypeList());
        resp.setValuesList(aggregateQueryResp.getValuesList());
        resp.setAggregateType(aggregateType);
        resp.setLimit(limit);
        resp.setOffset(offset);
        return resp;
    }

    private ExecuteSqlResp lastAggregateQuery(long sessionId) {
        if (endTime != Long.MAX_VALUE) {
            throw new SQLParserException("End time must be set as INF in aggregate query with last function.");
        }

        Core core = Core.getInstance();
        LastQueryReq req = new LastQueryReq(sessionId, getSelectedPaths(), startTime);
        LastQueryContext ctx = new LastQueryContext(req);
        core.processRequest(ctx);
        LastQueryResp lastQueryResp = ((LastQueryCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.AggregateQuery);
        resp.setPaths(lastQueryResp.getPaths());
        resp.setDataTypeList(lastQueryResp.getDataTypeList());
        resp.setTimestamps(lastQueryResp.getTimestamps());
        resp.setValuesList(lastQueryResp.getValuesList());
        resp.setAggregateType(AggregateType.LAST);
        resp.setLimit(limit);
        resp.setOffset(offset);
        return resp;
    }

    private ExecuteSqlResp downSampleQuery(long sessionId) {
        String funcName = selectedFuncsAndPaths.get(0).k;
        FuncType funcType = str2FuncType(funcName);
        AggregateType aggregateType = funcType2AggregateType(funcType);

        if (aggregateType == null || aggregateType.equals(AggregateType.FIRST) || aggregateType.equals(AggregateType.LAST)) {
            throw new SQLParserException(String.format("Not support function %s in downSample query for now.", funcName));
        }

        Core core = Core.getInstance();
        DownsampleQueryReq req = new DownsampleQueryReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime,
                aggregateType,
                precision
        );
        DownsampleQueryContext ctx = new DownsampleQueryContext(req);
        core.processRequest(ctx);
        DownsampleQueryResp downsampleQueryResp = ((DownsampleQueryCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.DownsampleQuery);
        resp.setPaths(downsampleQueryResp.getPaths());
        resp.setDataTypeList(downsampleQueryResp.getDataTypeList());
        resp.setQueryDataSet(downsampleQueryResp.getQueryDataSet());
        resp.setAggregateType(aggregateType);
        resp.setLimit(limit);
        resp.setOffset(offset);
        return resp;
    }

    private ExecuteSqlResp valueFilterQuery(long sessionId) {
        Core core = Core.getInstance();
        ValueFilterQueryReq req = new ValueFilterQueryReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime,
                booleanExpression
        );
        ValueFilterQueryContext ctx = new ValueFilterQueryContext(req);
        core.processRequest(ctx);
        ValueFilterQueryResp valueFilterQueryResp = ((ValueFilterCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.ValueFilterQuery);
        resp.setPaths(valueFilterQueryResp.getPaths());
        resp.setDataTypeList(valueFilterQueryResp.getDataTypeList());
        resp.setQueryDataSet(valueFilterQueryResp.getQueryDataSet());
        resp.setLimit(limit);
        resp.setOffset(offset);
        resp.setOrderByPath(orderByPath);
        resp.setAscending(ascending);
        return resp;
    }

    public enum FuncType {
        Null,
        First,
        Last,
        FirstValue,
        LastValue,
        Min,
        Max,
        Avg,
        Count,
        Sum,
        Udf,  // not support for now.
    }

    public enum QueryType {
        Unknown,
        SimpleQuery,
        AggregateQuery,
        MultiAggregateQuery,
        DownSampleQuery,
        MultiDownSampleQuery,
        ValueFilterQuery,
        ValueFilterAndDownSampleQuery,
        ValueFilterAndAggregateQuery,
        MixedQuery,
    }
}
