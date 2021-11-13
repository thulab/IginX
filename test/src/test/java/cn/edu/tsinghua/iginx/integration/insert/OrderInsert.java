package cn.edu.tsinghua.iginx.integration.insert;

import cn.edu.tsinghua.iginx.integration.delete.DeleteType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.LinkedList;

import static org.junit.Assert.fail;

public class OrderInsert extends BaseDataProcessType {

    public OrderInsert(Session session, int startTime, int endTime, boolean isColumnInsert, int deleteNum, int pathStart, int pathLength){
        this.session = session;
        this.startTime = startTime;
        this.endTime = endTime;
        this.isColumnInsert = isColumnInsert;
        this.deleteTypeNum = deleteNum;
        this.pathStart = pathStart;
        this.pathLength = pathLength;
        this.insertPaths = getPaths(pathStart, pathLength);
        this.validAggrPaths = getPaths(pathStart, pathLength);
    }

    @Override
    public void generateData() {
        int timePeriod = endTime - startTime + 1;
        int pathLen = insertPaths.size();
        this.timestamps = new long[timePeriod];
        for (long i = 0; i < timePeriod; i++) {
            timestamps[(int) i] = i + startTime;
        }

        if(isColumnInsert){
            this.valuesList = new Object[pathLen];
            for (int i = 0; i < pathLen; i++) {
                int pathNum = getPathNum(insertPaths.get(i));
                Object[] values = new Object[timePeriod];
                for (int j = 0; j < timePeriod; j++) {
                    values[j] = (long)(pathNum + j + startTime);
                }
                valuesList[i] = values;
            }
        } else {
            this.valuesList = new Object[timePeriod];
            for (int i = 0; i < timePeriod; i++) {
                Object[] values = new Object[pathLen];
                for (int j = 0; j < pathLen; j++) {
                    int pathNum = getPathNum(insertPaths.get(j));
                    values[j] = (long)(pathNum + i + startTime);
                }
                valuesList[i] = values;
            }
        }

        this.dataTypeList = new LinkedList<>();
        for (int i = 0; i < pathLen; i++) {
            dataTypeList.add(DataType.LONG);
        }
    }

    @Override
    public void generateDeleteData() {
        this.deleteType = new DeleteType(deleteTypeNum, startTime, endTime, insertPaths);
    }


    @Override
    public Object getQueryResult(int currTime, int pathNum) {
        if(startTime > currTime || endTime < currTime){
            return null;
        } else {
            if(!this.isDeleted || !isDeletedPath(pathNum)) {
                if (startTime <= currTime && currTime <= endTime) {
                    return (long) (currTime + pathNum);
                } else {
                    return null;
                }
            } else {
                if(this.deleteType.getDeleteStart() <= currTime && currTime <= this.deleteType.getDeleteEnd()){
                    return null;
                } else {
                    return (long)(currTime + pathNum);
                }
            }
        }
    }

    @Override
    public Object getAggrQueryResult(AggregateType aggrType, int startTime, int endTime, int pathNum) {

        boolean isDeleteUseful = true;

        int deleteStart = 0, deleteEnd = 0, realDeleteStart = 0, realDeleteEnd = 0;

        // 实际上的包含数据的区间的查询结果, 参数含义为这一段查询的起始和终止的时间
        int realEnd = Math.min(endTime, this.endTime);
        int realStart = Math.max(startTime, this.startTime);

        //接下来计算如果删除的情况下，被包含的删除的区间
        if (isDeleted && isDeletedPath(pathNum)){
            deleteStart = this.deleteType.getDeleteStart();
            deleteEnd = this.deleteType.getDeleteEnd();
            realDeleteStart = Math.max(deleteStart, realStart);
            realDeleteEnd = Math.min(deleteEnd, realEnd);
            if(realDeleteStart > realDeleteEnd){
                isDeleteUseful = false;
            }
        }

        //需要考虑到可能的问题：可能的实际的查询区间在插入数据之外？
        //TODO 比较关键的问题：如果相应的部分没有任何数据应该怎么处理？
        int timePeriod = realEnd - realStart + 1;
        int deletePeriod = realDeleteEnd - realDeleteStart + 1;

        double res = (realStart + realEnd) * timePeriod / 2.0;
        double delRes = (realDeleteStart + realDeleteEnd) * deletePeriod / 2.0;
        switch (aggrType) {
            case MAX:
            case LAST_VALUE:
                if(realStart > realEnd){
                    return null;
                }
                if (!isDeleted || !isDeletedPath(pathNum)|| !isDeleteUseful) {
                    return (realEnd + pathNum);
                } else {
                    //问题，如果全部删了会返回什么？
                    if (realDeleteEnd < realEnd){
                        return realEnd + pathNum;
                    } else if (realDeleteStart != realStart){
                        return realDeleteStart - 1 + pathNum;
                    } else {
                        return null;
                    }
                }
            case MIN:
            case FIRST_VALUE:
                if(realStart > realEnd){
                    return null;
                }
                if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                    return (realStart + pathNum);
                } else {
                    if (realDeleteStart > realStart){
                        return realStart + pathNum;
                    } else if (realDeleteEnd != realEnd){
                        return realDeleteEnd + 1 + pathNum;
                    } else {
                        return null;
                    }
                }
            case SUM:
                if(realStart > realEnd){
                    return 0.0;
                }
                if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                    return (res + pathNum * timePeriod);
                } else {
                    if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                        return (res - delRes + pathNum * (timePeriod - deletePeriod));
                    } else {
                        return 0.0;
                    }
                }
            case COUNT:
                if(realStart > realEnd){
                    return 0;
                }
                if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                    return timePeriod;
                } else {
                    if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                        return timePeriod - deletePeriod;
                    } else {
                        return 0;
                    }
                }
            case AVG:
                if(realStart > realEnd){
                    return null;
                }
                if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                    return (res / timePeriod + pathNum);
                } else {
                    if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                        return (res - delRes) / (timePeriod - deletePeriod) + pathNum;
                    } else {
                        return null;
                    }
                }
            default:
                fail();
                return null;
        }
    }

}


