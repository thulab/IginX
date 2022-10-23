package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.List;
import java.util.Map;

public class MultiConnection {

    private static Session session = null;

    private static SessionPool sessionPool = null;

    public MultiConnection(Session passedSession){
        session = passedSession;
    }

    public MultiConnection(SessionPool passedSessionPool){
        sessionPool = passedSessionPool;
    }

    public boolean isSession(){
        return session!=null;
    }

    public boolean isSessionPool(){
        return sessionPool!=null;
    }

    public void closeSession() throws SessionException {
        if(session!=null)
            session.closeSession();
        else if(sessionPool!=null)
            sessionPool.close();
    }

    public void openSession() throws SessionException {
        if(session!=null)
            session.openSession();
    }

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        if(session!=null)
            session.insertNonAlignedColumnRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
        else if(sessionPool!=null)
            sessionPool.insertNonAlignedColumnRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        if(session!=null)
            session.insertColumnRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
        else if(sessionPool!=null)
            sessionPool.insertColumnRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
    }

    public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                 List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        if(session!=null)
            session.insertRowRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
        else if(sessionPool!=null)
            sessionPool.insertRowRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
    }

    public void insertNonAlignedRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                           List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws SessionException, ExecutionException {
        if(session!=null)
            session.insertNonAlignedRowRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
        else if(sessionPool!=null)
            sessionPool.insertNonAlignedRowRecords(paths, timestamps,  valuesList, dataTypeList, tagsList);
    }

    public void deleteColumns(List<String> paths) throws SessionException, ExecutionException {
        if(session!=null)
            session.deleteColumns(paths);
        else if(sessionPool!=null)
            sessionPool.deleteColumns(paths);
    }

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType)
            throws SessionException, ExecutionException {
        if(session!=null)
            return session.aggregateQuery( paths, startTime,  endTime, aggregateType);
        else if(sessionPool!=null)
            return sessionPool.aggregateQuery( paths, startTime,  endTime, aggregateType);
        return null;
    }

    public SessionQueryDataSet downsampleQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision)
            throws SessionException, ExecutionException {
        if(session!=null)
            return session.downsampleQuery( paths, startTime,  endTime, aggregateType, precision);
        else if(sessionPool!=null)
            return sessionPool.downsampleQuery( paths, startTime,  endTime, aggregateType, precision);
        return null;
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime)
            throws SessionException, ExecutionException {
        if(session!=null)
            return session.queryData( paths, startTime,  endTime);
        else if(sessionPool!=null)
            return sessionPool.queryData( paths, startTime,  endTime);
        return null;
    }

    public SessionExecuteSqlResult executeSql(String statement) throws SessionException, ExecutionException {
        if(session!=null)
            return session.executeSql(statement);
        else if(sessionPool!=null)
            return sessionPool.executeSql(statement);
        return null;
    }

    public void addStorageEngine(String ip, int port, String type, Map<String, String> extraParams) throws SessionException, ExecutionException {
        if(session!=null)
            session.addStorageEngine(ip, port,  type, extraParams);
        else if(sessionPool!=null)
            sessionPool.addStorageEngine(ip, port,  type, extraParams);
    }

    public void deleteDataInColumns(List<String> paths, long startTime, long endTime) throws SessionException, ExecutionException {
        if(session!=null)
            session.deleteDataInColumns(paths,  startTime, endTime);
        else if(sessionPool!=null)
            sessionPool.deleteDataInColumns(paths,  startTime, endTime);
    }
}
