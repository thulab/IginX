package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
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

import java.util.*;

public class SelectStatement extends DataStatement {

    private QueryType queryType;

    private boolean hasFunc;
    private boolean hasValueFilter;
    private boolean hasGroupBy;
    private boolean ascending;

    private Map<String, List<String>> selectedFuncsAndPaths;
    private Set<FuncType> funcTypeSet;
    private Set<String> pathSet;
    private String fromPath;
    private String orderByPath;
    private Filter filter;
    //    private String booleanExpression;
//    private long startTime;
//    private long endTime;
    private long precision;
    private int limit;
    private int offset;

    public SelectStatement() {
        statementType = StatementType.SELECT;
        queryType = QueryType.Unknown;
        ascending = true;
        selectedFuncsAndPaths = new HashMap<>();
        funcTypeSet = new HashSet<>();
        fromPath = "";
        orderByPath = "";
//        startTime = Long.MIN_VALUE;
//        endTime = Long.MAX_VALUE;
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

    public boolean hasFunc() {
        return hasFunc;
    }

    public void setHasFunc(boolean hasFunc) {
        this.hasFunc = hasFunc;
    }

    public boolean hasValueFilter() {
        return hasValueFilter;
    }

    public void setHasValueFilter(boolean hasValueFilter) {
        this.hasValueFilter = hasValueFilter;
    }

    public boolean hasGroupBy() {
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
        selectedFuncsAndPaths.forEach((k, v) -> {
            paths.addAll(v);
        });
        return paths;
    }

    public Map<String, List<String>> getSelectedFuncsAndPaths() {
        return selectedFuncsAndPaths;
    }

    public void setSelectedFuncsAndPaths(String func, String path) {
        func = func.toLowerCase();
        String fullPath = fromPath + SQLConstant.DOT + path;
        List<String> pathList = this.selectedFuncsAndPaths.get(func);
        if (pathList == null) {
            pathList = new ArrayList<>();
            pathList.add(fullPath);
            this.selectedFuncsAndPaths.put(func, pathList);
        } else {
            pathList.add(fullPath);
        }

        FuncType type = str2FuncType(func);
        if (type != null) {
            this.funcTypeSet.add(type);
        }
    }

    public Set<FuncType> getFuncTypeSet() {
        return funcTypeSet;
    }

    public void setFuncTypeSet(Set<FuncType> funcTypeSet) {
        this.funcTypeSet = funcTypeSet;
    }

    public Set<String> getPathSet() {
        return pathSet;
    }

    public void setPathSet(String path) {
        this.pathSet.add(path);
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

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }
//    public String getBooleanExpression() {
//        return booleanExpression;
//    }

//    public void setBooleanExpression(String booleanExpression) {
//        this.booleanExpression = booleanExpression;
//    }

//    public long getStartTime() {
//        return startTime;
//    }
//
//    public void setStartTime(long startTime) {
//        this.startTime = startTime;
//    }
//
//    public long getEndTime() {
//        return endTime;
//    }
//
//    public void setEndTime(long endTime) {
//        this.endTime = endTime;
//    }

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
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("Select statement can not be executed directly.");
//        switch (queryType) {
//            case SimpleQuery:
//                return simpleQuery(sessionId);
//            case AggregateQuery:
//                return aggregateQuery(sessionId);
//            case DownSampleQuery:
//                return downSampleQuery(sessionId);
//            case ValueFilterQuery:
//                return valueFilterQuery(sessionId);
//            default:
//                ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.FAILURE, SqlType.NotSupportQuery);
//                resp.setParseErrorMsg(String.format("Not support %s for now.", queryType.toString()));
//                return resp;
//        }
    }

//    private ExecuteSqlResp simpleQuery(long sessionId) {
//        IginxWorker worker = IginxWorker.getInstance();
//        QueryDataReq req = new QueryDataReq(
//                sessionId,
//                getSelectedPaths(),
//                startTime,
//                endTime
//        );
//        QueryDataResp queryDataResp = worker.queryData(req);
//
//        checkIfHasOrderByPath(queryDataResp.getPaths());
//
//        ExecuteSqlResp resp = new ExecuteSqlResp(queryDataResp.getStatus(), SqlType.SimpleQuery);
//        resp.setPaths(queryDataResp.getPaths());
//        resp.setDataTypeList(queryDataResp.getDataTypeList());
//        resp.setQueryDataSet(queryDataResp.getQueryDataSet());
//        resp.setLimit(limit);
//        resp.setOffset(offset);
//        resp.setOrderByPath(orderByPath);
//        resp.setAscending(ascending);
//        return resp;
//    }

//    private ExecuteSqlResp aggregateQuery(long sessionId) {
//        String funcName = selectedFuncsAndPaths.get(0).k;
//        FuncType funcType = str2FuncType(funcName);
//        AggregateType aggregateType = funcType2AggregateType(funcType);
//
//        if (aggregateType == null || aggregateType.equals(AggregateType.FIRST)) {
//            throw new SQLParserException(String.format("Not support function %s in downSample query for now.", funcName));
//        }
//        if (aggregateType.equals(AggregateType.LAST)) {
//            return lastAggregateQuery(sessionId);
//        }
//
//        IginxWorker worker = IginxWorker.getInstance();
//        AggregateQueryReq req = new AggregateQueryReq(
//                sessionId,
//                getSelectedPaths(),
//                startTime,
//                endTime,
//                aggregateType
//        );
//        AggregateQueryResp aggregateQueryResp = worker.aggregateQuery(req);
//
//        ExecuteSqlResp resp = new ExecuteSqlResp(aggregateQueryResp.getStatus(), SqlType.AggregateQuery);
//        resp.setPaths(aggregateQueryResp.getPaths());
//        resp.setDataTypeList(aggregateQueryResp.getDataTypeList());
//        resp.setValuesList(aggregateQueryResp.getValuesList());
//        resp.setAggregateType(aggregateType);
//        resp.setLimit(limit);
//        resp.setOffset(offset);
//        return resp;
//    }

//    private ExecuteSqlResp lastAggregateQuery(long sessionId) {
//        if (endTime != Long.MAX_VALUE) {
//            throw new SQLParserException("End time must be set as INF in aggregate query with last function.");
//        }
//
//        IginxWorker worker = IginxWorker.getInstance();
//        LastQueryReq req = new LastQueryReq(sessionId, getSelectedPaths(), startTime);
//        LastQueryResp lastQueryResp = worker.lastQuery(req);
//
//        ExecuteSqlResp resp = new ExecuteSqlResp(lastQueryResp.getStatus(), SqlType.AggregateQuery);
//        resp.setPaths(lastQueryResp.getPaths());
//        resp.setDataTypeList(lastQueryResp.getDataTypeList());
//        resp.setTimestamps(lastQueryResp.getTimestamps());
//        resp.setValuesList(lastQueryResp.getValuesList());
//        resp.setAggregateType(AggregateType.LAST);
//        resp.setLimit(limit);
//        resp.setOffset(offset);
//        return resp;
//    }

//    private ExecuteSqlResp downSampleQuery(long sessionId) {
//        String funcName = selectedFuncsAndPaths.get(0).k;
//        FuncType funcType = str2FuncType(funcName);
//        AggregateType aggregateType = funcType2AggregateType(funcType);
//
//        if (aggregateType == null || aggregateType.equals(AggregateType.FIRST) || aggregateType.equals(AggregateType.LAST)) {
//            throw new SQLParserException(String.format("Not support function %s in downSample query for now.", funcName));
//        }
//
//        IginxWorker worker = IginxWorker.getInstance();
//        DownsampleQueryReq req = new DownsampleQueryReq(
//                sessionId,
//                getSelectedPaths(),
//                startTime,
//                endTime,
//                aggregateType,
//                precision
//        );
//        DownsampleQueryResp downsampleQueryResp = worker.downsampleQuery(req);
//
//        ExecuteSqlResp resp = new ExecuteSqlResp(downsampleQueryResp.getStatus(), SqlType.DownsampleQuery);
//        resp.setPaths(downsampleQueryResp.getPaths());
//        resp.setDataTypeList(downsampleQueryResp.getDataTypeList());
//        resp.setQueryDataSet(downsampleQueryResp.getQueryDataSet());
//        resp.setAggregateType(aggregateType);
//        resp.setLimit(limit);
//        resp.setOffset(offset);
//        return resp;
//    }

//    private ExecuteSqlResp valueFilterQuery(long sessionId) {
//        IginxWorker worker = IginxWorker.getInstance();
//        ValueFilterQueryReq req = new ValueFilterQueryReq(
//                sessionId,
//                getSelectedPaths(),
//                startTime,
//                endTime,
//                booleanExpression
//        );
//        ValueFilterQueryResp valueFilterQueryResp = worker.valueFilterQuery(req);
//
//        checkIfHasOrderByPath(valueFilterQueryResp.getPaths());
//
//        ExecuteSqlResp resp = new ExecuteSqlResp(valueFilterQueryResp.getStatus(), SqlType.ValueFilterQuery);
//        resp.setPaths(valueFilterQueryResp.getPaths());
//        resp.setDataTypeList(valueFilterQueryResp.getDataTypeList());
//        resp.setQueryDataSet(valueFilterQueryResp.getQueryDataSet());
//        resp.setLimit(limit);
//        resp.setOffset(offset);
//        resp.setOrderByPath(orderByPath);
//        resp.setAscending(ascending);
//        return resp;
//    }

//    private void checkIfHasOrderByPath(List<String> paths) {
//        if (!orderByPath.equals("") && !paths.contains(orderByPath)) {
//            throw new SQLParserException(String.format("Selected paths did not contain '%s'.", orderByPath));
//        }
//    }

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
