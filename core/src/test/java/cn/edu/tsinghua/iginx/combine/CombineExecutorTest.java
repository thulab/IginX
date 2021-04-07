package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.tsinghua.iginx.core.context.ContextType.QueryData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class CombineExecutorTest {
    CombineExecutor c;

    @Before
    public void init() {
         c = new CombineExecutor();
    }

    @Test
    public void testCombineResult() throws Exception {

        SnowFlakeUtils.init(0);
        RequestContext r = new RequestContext(0, QueryData);
        List<PlanExecuteResult> planExecuteResults = new LinkedList<>();

        List<String> columnNameList = new LinkedList<>();
        columnNameList.add("time");
        columnNameList.add("a");
        columnNameList.add("b");
        List<DataType> columnTypeList = new LinkedList<>();
        columnTypeList.add(DataType.LONG);
        columnTypeList.add(DataType.STRING);
        columnTypeList.add(DataType.INTEGER);
        List<Long> timeStamps = new LinkedList<>();
        timeStamps.add((long) 0);
        timeStamps.add((long) 100);
        timeStamps.add((long) 200);
        List<List<Object>> result = new LinkedList<>();
        List<Object> line1 = new LinkedList<>();
        line1.add("test1".getBytes());
        line1.add(0);
        result.add(line1);
        List<Object> line2 = new LinkedList<>();
        line2.add("test2".getBytes());
        line2.add(100);
        result.add(line2);
        List<Object> line3 = new LinkedList<>();
        line3.add("test3".getBytes());
        line3.add(200);
        result.add(line3);
        QueryExecuteDataSet dataSet = new simpleQueryExecuteDataSet(
                columnTypeList, columnNameList, timeStamps, result
                );
        List<String> path = new LinkedList<>();
        path.add("root.a.a.a");
        path.add("root.a.a.b");
        QueryDataPlan plan = new QueryDataPlan(path, 0, 210);
        List<QueryExecuteDataSet> set1 = new LinkedList<>();
        set1.add(dataSet);
        PlanExecuteResult per1 = new
                QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan, set1);
        planExecuteResults.add(per1);

        List<String> columnNameList2 = new LinkedList<>();
        columnNameList2.add("time");
        columnNameList2.add("b");
        columnNameList2.add("c");
        List<DataType> columnTypeList2 = new LinkedList<>();
        columnTypeList2.add(DataType.LONG);
        columnTypeList2.add(DataType.INTEGER);
        columnTypeList2.add(DataType.STRING);
        List<Long> timeStamps2 = new LinkedList<>();
        timeStamps2.add((long) 250);
        timeStamps2.add((long) 300);
        List<List<Object>> result2 = new LinkedList<>();
        List<Object> line21 = new LinkedList<>();
        line21.add(1);
        line21.add("test21".getBytes());
        result2.add(line21);
        List<Object> line22 = new LinkedList<>();
        line22.add(2);
        line22.add("test22".getBytes());
        result2.add(line22);
        QueryExecuteDataSet dataSet2 = new simpleQueryExecuteDataSet(
                columnTypeList2, columnNameList2, timeStamps2, result2
        );
        List<String> path2 = new LinkedList<>();
        path2.add("root.a.a.b");
        path2.add("root.a.a.c");
        QueryDataPlan plan2 = new QueryDataPlan(path2, 250, 310);
        List<QueryExecuteDataSet> set2 = new LinkedList<>();
        set2.add(dataSet2);
        PlanExecuteResult per2 = new
                QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan2, set2);
        planExecuteResults.add(per2);
        r.setPlanExecuteResults(planExecuteResults);
        QueryDataCombineResult res = (QueryDataCombineResult) c.combineResult(r);

        List<DataType> dt = res.getResp().dataTypeList;
        List<String> dn = res.getResp().getPaths();
        System.out.println("dt="+dt);
        System.out.println("dn="+dn);

        QueryDataSet q = res.getResp().getQueryDataSet();
        byte[] timeStampByte = q.getTimestamps();
        long[] timeStamp = ByteUtils.getLongArrayFromByteArray(timeStampByte);
        List<ByteBuffer> val = q.getValuesList();
        List<ByteBuffer> bit = q.getBitmapList();
        assertEquals(timeStamp.length, 5);
        assertEquals(val.size(), 5);
        assertEquals(bit.size(), 5);

        for(int i = 0; i < timeStamp.length; i++) {
            long time = timeStamp[i];
            System.out.print(time+ " ");
            ByteBuffer bitmap = q.getBitmapList().get(i);
            ByteBuffer value = q.getValuesList().get(i);
            byte[] bytes = new byte[bitmap.remaining()];
            int size = bytes.length * 8;
            Bitmap b = new Bitmap(size, bytes);
            bitmap.get(bytes, 0, bytes.length);
            for (int j = 0; j < size; j++){
                if (b.get(j)) {
                    System.out.print(j + ":");
                    if(dt.get(j) == DataType.STRING) {
                        //only line 1 is integer，else are Strings
                        assertNotEquals(j, 1);
                        String queryResult = new String((byte[]) ByteUtils.getValueByDataType(value, dt.get(j)));
                        if(time <= 200){
                            assertEquals("test"+((time + 100) / 100), queryResult);
                        } else {
                            assertEquals("test2"+((time - 200) / 50) , queryResult);
                        }
                        System.out.print(queryResult + " ");
                    } else {
                        assertEquals(j,1);
                        long queryResult = (long)(Integer)ByteUtils.getValueByDataType(value, dt.get(j));
                        if(time <= 200){
                            assertEquals(time, queryResult);
                        } else {
                            assertEquals((time - 200) / 50 , queryResult);
                        }
                        System.out.print(queryResult + " ");
                    }
                }
            }
            System.out.println();
        }
    }

}
