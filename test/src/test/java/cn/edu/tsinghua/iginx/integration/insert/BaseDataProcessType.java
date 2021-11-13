package cn.edu.tsinghua.iginx.integration.insert;

import cn.edu.tsinghua.iginx.integration.delete.DeleteType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseDataProcessType {

    protected Session session;
    protected int startTime;
    protected int endTime;//[startTime, endTime]
    protected boolean isColumnInsert;
    protected List<String> insertPaths;
    protected List<String> validAggrPaths;
    protected DeleteType deleteType;
    protected boolean isDeleted;
    protected long[] timestamps;
    protected Object[] valuesList;
    protected List<DataType> dataTypeList;

    protected int pathStart;
    protected int pathLength;

    protected int deleteTypeNum;

    public abstract void generateData();

    public abstract void generateDeleteData();

    public void insert(){
        try {
            generateData();
            if (isColumnInsert) {
                session.insertNonAlignedColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
            } else {
                session.insertNonAlignedRowRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
            }
        } catch (Exception e) {
            System.out.println("Error occurs in baseInsertTime, where isColumnInsert = "+ isColumnInsert);
            e.printStackTrace();
        }
    }

    public void delete(){
        generateDeleteData();
        try {
            session.deleteDataInColumns(deleteType.getDeletePath(),
                    deleteType.getDeleteStart(), deleteType.getDeleteEnd() + 1);
            isDeleted = true;
        } catch (Exception e) {
            System.out.println("Error occurs in baseDeleteTime, where delStartTime="+ deleteType.getDeleteStart()
                    +" delEndTime=" + deleteType.getDeleteEnd() + " ,deletePaths=" + deleteType.getDeletePath().toString());
            e.printStackTrace();
        }
    }

    public List<String> getInsertPaths() {
        return insertPaths;
    }

    public List<String> getValidAggrPaths() {
        return validAggrPaths;
    }

    public BaseDataProcessType(){
        isDeleted = false;
    }

    public abstract Object getQueryResult(int currTime, int pathNum);

    //assume that there is only one delete in the query
    public abstract Object getAggrQueryResult(AggregateType aggrType, int startTime, int endTime, int pathNum);

    public Object[] getDownSampleQueryResult(AggregateType aggrType, int startTime, int endTime, int pathNum, int period, boolean isFromZero){
        //isFromZero指区间从什么位置开始
        int actualStart;
        int windowNum;
        if(isFromZero){
            actualStart = (startTime / period) * period;
            int actualEnd = (endTime / period + 1) * period; //定义为一个实际值而不是一个虚拟值
            windowNum = (actualEnd - actualStart) / period;
        } else {
            actualStart = startTime;
            windowNum = (endTime - startTime) / period + 1;
        }
        // 计算区间个数
        Object[] result = new Object[windowNum];
        for(int i = 0; i < windowNum; i++){
            result[i] = getAggrQueryResult(aggrType, actualStart + i * period, actualStart +
                    (i + 1) * period - 1, pathNum);
        }
        return result;
    }

    //generate PathNum by the type order
    protected int getPathNum(String path) {
        String pattern = "^sg1\\.d(\\d+)\\.s(\\d+)$";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(path);
        if (m.find()) {
            int d = Integer.parseInt(m.group(1));
            int s = Integer.parseInt(m.group(2));
            if (d == s) {
                return d;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    protected List<String> getPaths(int startPosition, int len) {
        List<String> paths = new ArrayList<>();
        for(int i = startPosition; i < startPosition + len; i++){
            paths.add("sg1.d" + i + ".s" + i);
        }
        return paths;
    }

    protected String getSinglePath(int position) {
        return ("sg1.d" + position + ".s" + position);
    }

    protected boolean isDeletedPath(int pathNum){
        if(!isDeleted){
            return false;
        }
        List<String> paths = deleteType.getDeletePath();
        for (String path : paths) {
            if (path.equals(getSinglePath(pathNum))) {
                return true;
            }
        }
        return false;
    }

    public int getPathStart() {
        return pathStart;
    }

    public int getPathLength() {
        return pathLength;
    }

    public List<DataType> getDataTypeList() {
        return dataTypeList;
    }
}
