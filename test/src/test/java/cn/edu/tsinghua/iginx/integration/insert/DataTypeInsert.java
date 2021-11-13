package cn.edu.tsinghua.iginx.integration.insert;

import cn.edu.tsinghua.iginx.integration.delete.DeleteType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.LinkedList;
import java.util.Random;

import static org.junit.Assert.fail;

public class DataTypeInsert extends BaseDataProcessType {

    protected final String RAND_STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    protected final int STRING_LEN = 1000;

    public DataTypeInsert(Session session, int startTime, int endTime, boolean isColumnInsert, int deleteNum, int pathStart){
        this.session = session;
        this.startTime = startTime;
        this.endTime = endTime;
        this.isColumnInsert = isColumnInsert;
        this.deleteTypeNum = deleteNum;
        this.pathStart = pathStart;
        this.pathLength = 6; // 6 basic data type, fixed
        this.insertPaths = getPaths(pathStart, pathLength);
        this.validAggrPaths = getPaths(pathStart + 1, pathLength - 2);
    }

    @Override
    public void generateData() {
        int timePeriod = endTime - startTime + 1;
        this.timestamps = new long[timePeriod];
        for (long i = 0; i < timePeriod; i++) {
            timestamps[(int) i] = i + startTime;
        }
        valuesList = new Object[pathLength];
        for (int i = 0; i < pathLength; i++) {
            Object[] values = new Object[timePeriod];
            int pathNum = getPathNum(insertPaths.get(i)) - pathStart;
            for (int j = 0; j < timePeriod; j++) {
                switch (pathNum) {
                    case 1:
                        //integer
                        values[j] = pathNum + (timePeriod - j - 1) + startTime;
                        break;
                    case 2:
                        //long
                        values[j] = (pathNum + j + startTime) * 1000L;
                        break;
                    case 3:
                        values[j] = (pathNum + j + startTime) + 0.01F;
                        //float
                        break;
                    case 4:
                        //double
                        values[j] = (pathNum + (timePeriod - j - 1) + startTime + 0.01) * 999;
                        break;
                    case 5:
                        //binary
                        values[j] = getRandomStr(j, STRING_LEN).getBytes();
                        break;
                    default:
                        //boolean
                        values[j] = (j % 2 == 0);
                        break;
                }
            }
            valuesList[i] = values;
        }
        dataTypeList = new LinkedList<>();
        for (int i = 0; i < 6; i++) {
            dataTypeList.add(DataType.findByValue(i));
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
                    return getDataTypeResult(currTime, pathNum);
                } else {
                    return null;
                }
            } else {
                if(this.deleteType.getDeleteStart() <= currTime && currTime <= this.deleteType.getDeleteEnd()){
                    return null;
                } else {
                    return getDataTypeResult(currTime, pathNum);
                }
            }
        }
    }

    @Override
    public Object getAggrQueryResult(AggregateType aggrType, int startTime, int endTime, int pathNum) {

        //TODO it must ensure the path can be aggregrate

        boolean isDeleteUseful = true;

        int deleteStart = 0, deleteEnd = 0, realDeleteStart = 0, realDeleteEnd = 0;

        // 实际上的包含数据的区间的查询结果, 参数含义为这一段查询的起始和终止的时间
        int realEnd = Math.min(endTime, this.endTime);
        int realStart = Math.max(startTime, this.startTime);

        // 接下来计算如果删除的情况下，被包含的删除的区间
        if (isDeleted && isDeletedPath(pathNum)){
            deleteStart = this.deleteType.getDeleteStart();
            deleteEnd = this.deleteType.getDeleteEnd();
            realDeleteStart = Math.max(deleteStart, realStart);
            realDeleteEnd = Math.min(deleteEnd, realEnd);
            if(realDeleteStart > realDeleteEnd){
                isDeleteUseful = false;
            }
        }

        int timePeriod = realEnd - realStart + 1;
        int deletePeriod = realDeleteEnd - realDeleteStart + 1;

        double res = getDataTypeSumResult(realStart, realEnd, pathNum);
        double delRes = getDataTypeSumResult(realDeleteStart , realDeleteEnd, pathNum);

        int currPathPos = pathNum - pathStart;

        switch (currPathPos){
            //reversed
            case 1:
            case 4:
                switch (aggrType) {
                    case MIN:
                    case LAST_VALUE:
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return getDataTypeResult(realEnd, pathNum);
                        } else {
                            if (realDeleteEnd < realEnd) {
                                return getDataTypeResult(realEnd, pathNum);
                            } else if (realDeleteStart != realStart) {
                                return getDataTypeResult(realDeleteStart - 1, pathNum);
                            } else {
                                return null;
                            }
                        }
                    case MAX:
                    case FIRST_VALUE:
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return getDataTypeResult(realStart, pathNum);
                        } else {
                            if (realDeleteStart > realStart) {
                                return getDataTypeResult(realStart, pathNum);
                            } else if (realDeleteEnd != realEnd) {
                                return getDataTypeResult(realDeleteEnd + 1, pathNum);
                            } else {
                                return null;
                            }
                        }
                    case SUM:
                        if (realStart > realEnd) {
                            return 0.0;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return res;
                        } else {
                            if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                                return res - delRes;
                            } else {
                                return 0.0;
                            }
                        }
                    case COUNT:
                        if (realStart > realEnd) {
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
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return res / timePeriod;
                        } else {
                            if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                                return (res - delRes) /  (timePeriod - deletePeriod);
                            } else {
                                return null;
                            }
                        }
                    default:
                        fail();
                        return null;
                }
            //ordered
            case 2:
            case 3:
                switch (aggrType) {
                    case MAX:
                    case LAST_VALUE:
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return getDataTypeResult(realEnd, pathNum);
                        } else {
                            if (realDeleteEnd < realEnd) {
                                return getDataTypeResult(realEnd, pathNum);
                            } else if (realDeleteStart != realStart) {
                                return getDataTypeResult(realDeleteStart - 1, pathNum);
                            } else {
                                return null;
                            }
                        }
                    case MIN:
                    case FIRST_VALUE:
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return getDataTypeResult(realStart, pathNum);
                        } else {
                            if (realDeleteStart > realStart) {
                                return getDataTypeResult(realStart, pathNum);
                            } else if (realDeleteEnd != realEnd) {
                                return getDataTypeResult(realDeleteEnd + 1, pathNum);
                            } else {
                                return null;
                            }
                        }
                    case SUM:
                        if (realStart > realEnd) {
                            return 0.0;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return res;
                        } else {
                            if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                                return (res - delRes);
                            } else {
                                return 0.0;
                            }
                        }
                    case COUNT:
                        if (realStart > realEnd) {
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
                        if (realStart > realEnd) {
                            return null;
                        }
                        if (!isDeleted || !isDeletedPath(pathNum) || !isDeleteUseful) {
                            return (res / timePeriod);
                        } else {
                            if (realDeleteStart != realStart || realDeleteEnd != realEnd) {
                                return (res - delRes) / (timePeriod - deletePeriod);
                            } else {
                                return null;
                            }
                        }
                    default:
                        fail();
                        return null;
                }
            default:
                fail();
                return null;
        }
    }

    protected String getRandomStr(int seed, int length) {
        Random random = new Random(seed);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            int number = random.nextInt(RAND_STR.length());
            sb.append(RAND_STR.charAt(number));
        }
        return sb.toString();
    }

    private Object getDataTypeResult(int currTime, int pathNum) {
        //ensure that this position have the result
        int currPathPos = pathNum - pathStart;
        switch (currPathPos) {
            case 0:
                return ((currTime - startTime) % 2 == 0);
            case 1:
                return (endTime - (currTime - startTime)) + 1;
            case 2:
                return (currTime + 2L) * 1000;
            case 3:
                return (currTime + 3 + 0.01F);
            case 4:
                return (endTime - (currTime - startTime) + 4 + 0.01) * 999;
            case 5:
                return getRandomStr((currTime - startTime), STRING_LEN).getBytes();
            default:
                fail();
                return null;
        }
    }

    private double getDataTypeSumResult(int sumStart, int sumEnd, int pathNum) {
        //Ensure that the all time in this period have data
        int currPathPos = pathNum - pathStart;
        switch (currPathPos) {
            case 1:
                return (endTime - (sumStart - startTime) + 1 + endTime - (sumEnd - startTime) + 1) * (sumEnd -
                        sumStart + 1) / 2.0;
            case 2:
                return (sumStart + 2 + sumEnd + 2) * (sumEnd - sumStart + 1) * 500.0;
            case 3:
                return (sumStart + 3 + 0.01 + sumEnd + 3 + 0.01) * (sumEnd - sumStart + 1) / 2;
            case 4:
                return (endTime - (sumStart - startTime) + 4 + 0.01 + endTime - (sumEnd - startTime) + 4 + 0.01)
                        * (sumEnd - sumStart + 1) * 999 / 2;
            default:
                fail();
                return 0.0;
        }
    }
}


