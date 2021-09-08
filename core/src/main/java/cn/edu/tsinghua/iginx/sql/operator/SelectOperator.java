package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.combine.AggregateCombineResult;
import cn.edu.tsinghua.iginx.combine.DownsampleQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.combine.ValueFilterCombineResult;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectOperator extends Operator {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupBy;

    private List<Pair<FuncType, String>> selectedFuncsAndPaths;
    private Set<FuncType> funcTypeSet;
    private String fromPath;
    private String booleanExpression;
    private long startTime;
    private long endTime;
    private long precision;
    private int limit;
    private int offset;

    public SelectOperator() {
        this.operatorType = OperatorType.SELECT;
        this.queryType = QueryType.Unknown;
        selectedFuncsAndPaths = new LinkedList<>();
        funcTypeSet = new HashSet<>();
        fromPath = "";
        startTime = Long.MIN_VALUE;
        endTime = Long.MAX_VALUE;
        limit = Integer.MAX_VALUE;
        offset = 0;
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

    public List<String> getSelectedPaths() {
        List<String> paths = new ArrayList<>();
        for (Pair<FuncType, String> kv : selectedFuncsAndPaths) {
            paths.add(kv.v);
        }
        return paths;
    }

    public List<Pair<FuncType, String>> getSelectedFuncsAndPaths() {
        return selectedFuncsAndPaths;
    }

    public void setSelectedFuncsAndPaths(FuncType type, String path) {
        this.selectedFuncsAndPaths.add(new Pair<>(type, fromPath + SQLConstant.DOT + path));
        this.funcTypeSet.add(type);
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

    public static FuncType str2FuncType(String str) {
        switch (str.toLowerCase()) {
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
            default:
                return FuncType.Udf;
        }
    }

    public static AggregateType funcType2AggregateType(FuncType type) {
        switch (type) {
            case First:
                return AggregateType.FIRST_VALUE;
            case Last:
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
        return resp;
    }

    private ExecuteSqlResp aggregateQuery(long sessionId) {
        Core core = Core.getInstance();
        AggregateQueryReq req = new AggregateQueryReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime,
                funcType2AggregateType(selectedFuncsAndPaths.get(0).k)
        );
        AggregateQueryContext ctx = new AggregateQueryContext(req);
        core.processRequest(ctx);
        AggregateQueryResp aggregateQueryResp = ((AggregateCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.AggregateQuery);
        resp.setPaths(aggregateQueryResp.getPaths());
        resp.setDataTypeList(aggregateQueryResp.getDataTypeList());
        resp.setTimestamps(aggregateQueryResp.getTimestamps());
        resp.setValuesList(aggregateQueryResp.getValuesList());
        resp.setAggregateType(funcType2AggregateType(selectedFuncsAndPaths.get(0).k));
        resp.setLimit(limit);
        resp.setOffset(offset);
        return resp;
    }

    private ExecuteSqlResp downSampleQuery(long sessionId) {
        Core core = Core.getInstance();
        DownsampleQueryReq req = new DownsampleQueryReq(
                sessionId,
                getSelectedPaths(),
                startTime,
                endTime,
                funcType2AggregateType(selectedFuncsAndPaths.get(0).k),
                precision
        );
        DownsampleQueryContext ctx = new DownsampleQueryContext(req);
        core.processRequest(ctx);
        DownsampleQueryResp downsampleQueryResp = ((DownsampleQueryCombineResult) ctx.getCombineResult()).getResp();

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.DownsampleQuery);
        resp.setPaths(downsampleQueryResp.getPaths());
        resp.setDataTypeList(downsampleQueryResp.getDataTypeList());
        resp.setQueryDataSet(downsampleQueryResp.getQueryDataSet());
        resp.setAggregateType(funcType2AggregateType(selectedFuncsAndPaths.get(0).k));
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
        return resp;
    }

    public enum FuncType {
        Null,
        First,
        Last,
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
