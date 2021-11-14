package cn.edu.tsinghua.iginx.integration.insert;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class MultiThreadInsert extends OrderInsert {

    // 多线程包含2种拆分方式：沿着时间拆分/沿着路径拆分。
    protected boolean isInsertedByTime;
    protected boolean isDeletedByTime;
    protected int threadNum;

    public MultiThreadInsert(Session session, int startTime, int endTime, boolean isColumnInsert,
                             int deleteNum, int pathStart, int pathLength, boolean isInsertedByTime,
                             boolean isDeletedByTime, int threadNum) {
        super(session, startTime, endTime, isColumnInsert, deleteNum, pathStart, pathLength);
        this.isInsertedByTime = isInsertedByTime;
        this.isDeletedByTime = isDeletedByTime;
        this.threadNum = threadNum;
    }

    @Override
    public void insert(){
        generateData();
        try{
            MultiThreadTask[] mulStInsertTasks = new MultiThreadTask[threadNum];
            Thread[] mulStInsertThreads = new Thread[threadNum];
            if(isInsertedByTime) {
                int period = (endTime - startTime + 1) / threadNum;
                for (int i = 0; i < threadNum; i++) {
                    int threadStart = startTime + i * period;
                    int threadEnd = (i == threadNum - 1) ? endTime : startTime + (i + 1) * period - 1;
                    // TODO Session??
                    mulStInsertTasks[i] = new MultiThreadTask(1, insertPaths, threadStart,
                            threadEnd, (threadEnd - threadStart + 1), 1, null, 6888);
                    mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
                }
            } else {
                int pathLen = pathLength / threadNum;
                for (int i = 0; i < threadNum; i++) {
                    int realStart = pathStart + i * pathLen;
                    int realPathLen = (i == threadNum - 1) ? pathLength - (threadNum - 1) * pathLen : pathLen;
                    mulStInsertTasks[i] = new MultiThreadTask(1, getPaths(realStart, realPathLen), startTime,
                            endTime, (endTime - startTime + 1), 1, null, 6888);
                    mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
                }
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].start();
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].join();
            }
            Thread.sleep(1000);
        } catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void delete(){
        generateDeleteData();
        try {
            MultiThreadTask[] mulStInsertTasks = new MultiThreadTask[threadNum];
            Thread[] mulStInsertThreads = new Thread[threadNum];
            int delStart = deleteType.getDeleteStart();
            int delEnd = deleteType.getDeleteEnd();
            List<String> delPaths = deleteType.getDeletePath();
            int pathNum = delPaths.size();
            if(isDeletedByTime){
                int period = (delEnd - delStart + 1) / threadNum;
                for (int i = 0; i < threadNum; i++) {
                    int threadStart = delStart + i * period;
                    int threadEnd = (i == threadNum - 1) ? delEnd : delStart + (i + 1) * period - 1;
                    mulStInsertTasks[i] = new MultiThreadTask(2, delPaths, threadStart,
                            threadEnd, (threadEnd - threadStart + 1), 1, null, 6888);
                    mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
                }
            } else {
                int pathLen = pathNum / threadNum;
                for (int i = 0; i < threadNum; i++) {
                    int realStart = i * pathLen;
                    int realPathLen = (i == threadNum - 1) ? pathNum - (threadNum - 1) * pathLen : pathLen;
                    List<String> threadDelPath = new LinkedList<>();
                    for(int j = 0; j < realPathLen; j++){
                        threadDelPath.add(delPaths.get(j + realStart));
                    }
                    mulStInsertTasks[i] = new MultiThreadTask(2, threadDelPath, delStart,
                            delEnd, (delEnd - delStart + 1), 1, null, 6888);
                    mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
                }
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].start();
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].join();
            }
            Thread.sleep(1000);
            isDeleted = true;
        } catch (Exception e) {
            System.out.println("Error occurs in DeleteTime, where delStartTime="+ deleteType.getDeleteStart()
                    +" delEndTime=" + deleteType.getDeleteEnd() + " ,deletePaths=" + deleteType.getDeletePath().toString());
            e.printStackTrace();
        }
    }

    //返回的结果为SessionQueryDataSet或SessionAggregateQueryDataSet
    public List<Object> multiThreadQuery(List<List<String>> paths,
                                                             List<Integer> startTimes, List<Integer> endTimes,
                                                             List<AggregateType> aggrTypes) {
        List<Object> queryResult = new ArrayList<>();
        try {
            //在外层切分，直接在内部执行切分之后的结果。这一接口可以同样执行query或aggregateQuery
            MultiThreadTask[] mulStInsertTasks = new MultiThreadTask[threadNum];
            Thread[] mulStInsertThreads = new Thread[threadNum];
            for (int i = 0; i < threadNum; i++) {
                mulStInsertTasks[i] = new MultiThreadTask(3, paths.get(i), startTimes.get(i),
                        endTimes.get(i), endTimes.get(i) - startTimes.get(i) + 1, 1,
                        aggrTypes.get(i), 6888);
                mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].start();
            }
            for (int i = 0; i < threadNum; i++) {
                mulStInsertThreads[i].join();
            }
            for(int i = 0; i < threadNum; i++){
                queryResult.add(mulStInsertTasks[i].getQueryDataSet());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return queryResult;
    }

    private class MultiThreadTask implements Runnable {

        //1:insert 2:delete 3:query
        private int type;
        private long startTime;
        private long endTime;
        private long pointNum;
        private int step;
        private List<String> path;
        private Object queryDataSet;
        private AggregateType aggregateType;

        private Session localSession;

        public MultiThreadTask(int type, List<String> path, long startTime, long endTime,
                               long pointNum, int step, AggregateType aggrType, int portNum) throws SessionException {
            this.type = type;
            this.path = new ArrayList(path);
            this.startTime = startTime;
            this.endTime = endTime;
            this.pointNum = pointNum;
            this.step = step;
            this.queryDataSet = null;
            this.aggregateType = aggrType;
            this.localSession = new Session("127.0.0.1", portNum,
                    "root", "root");
            this.localSession.openSession();
        }

        @Override
        public void run() {
            switch (type){
                //insert
                case 1:
                    long[] timestamps = new long[(int)pointNum];
                    for (long i = 0; i < pointNum; i++) {
                        timestamps[(int) i] = startTime + step * i;
                    }
                    int pathSize = path.size();
                    Object[] valuesList = new Object[pathSize];
                    for (int i = 0; i < pathSize; i++) {
                        Object[] values = new Object[(int)pointNum];
                        for (int j = 0; j < pointNum; j++) {
                            values[j] = timestamps[j] + getPathNum(path.get(i));
                        }
                        valuesList[i] = values;
                    }
                    List<DataType> dataTypeList = new ArrayList<>();
                    for (int i = 0; i < pathSize; i++) {
                        dataTypeList.add(DataType.LONG);
                    }
                    try {
                        localSession.insertNonAlignedColumnRecords(path, timestamps, valuesList, dataTypeList, null);
                    } catch (SessionException | ExecutionException e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                // delete
                case 2:
                    try {
                        localSession.deleteDataInColumns(path, startTime, endTime + 1);
                    } catch(SessionException | ExecutionException e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                //query
                case 3:
                    try {
                        if (aggregateType == null) {
                            queryDataSet = localSession.queryData(path, startTime, endTime + 1);
                        } else {
                            queryDataSet = localSession.aggregateQuery(path, startTime, endTime + 1, aggregateType);
                        }
                    } catch(SessionException | ExecutionException e) {
                        System.out.println(e.getMessage());
                    }
                    break;
                default:
                    break;
            }
            try {
                this.localSession.closeSession();
            } catch (SessionException e){
                System.out.println(e.getMessage());
            }
        }

        public Object getQueryDataSet() {
            return queryDataSet;
        }
    }

    public int getThreadNum() {
        return threadNum;
    }
}
