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
    int size1 = 300;
    int size2 = 200;

    @Before
    public void init() {
         c = new CombineExecutor();
    }

    @Test
    public void testCombineResult() throws Exception {

        SnowFlakeUtils.init(0);
        RequestContext r = new RequestContext(0, QueryData);
        List<PlanExecuteResult> planExecuteResults = new LinkedList<>();

        //Build query result1,with column a and column b, timestamp 0, 100, 200

        //Create column, initialize its name and type
        List<String> columnNameList = new LinkedList<>();
        columnNameList.add("time");
        columnNameList.add("a");
        columnNameList.add("b");
        List<DataType> columnTypeList = new LinkedList<>();
        columnTypeList.add(DataType.LONG);
        columnTypeList.add(DataType.BINARY);
        columnTypeList.add(DataType.INTEGER);

        // Create timestamp and add results
        List<Long> timeStamps = new LinkedList<>();
        List<List<Object>> result = new LinkedList<>();
        for(int i = 0; i < size1; i++) {
            List<Object> line = new LinkedList<>();
            line.add(("test" + (i + 1)).getBytes());
            line.add(i * 100);
            timeStamps.add((long) (i * 100));
            result.add(line);
        }

        QueryExecuteDataSet dataSet = new SimpleQueryExecuteDataSet(
                columnTypeList, columnNameList, timeStamps, result
                );

        List<String> path = new LinkedList<>();
        path.add("root.a.a.a");
        path.add("root.a.a.b");
        QueryDataPlan plan = new QueryDataPlan(path, 0, size1 * 100);
        List<QueryExecuteDataSet> set1 = new LinkedList<>();
        set1.add(dataSet);
        PlanExecuteResult per1 = new
                QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan, set1);
        planExecuteResults.add(per1);

        // Build query result2, with column b and column c, timestamp 250, 300

        // Create column, initialize its name and type
        List<String> columnNameList2 = new LinkedList<>();
        columnNameList2.add("time");
        columnNameList2.add("b");
        columnNameList2.add("c");
        List<DataType> columnTypeList2 = new LinkedList<>();
        columnTypeList2.add(DataType.LONG);
        columnTypeList2.add(DataType.INTEGER);
        columnTypeList2.add(DataType.BINARY);

        // Create timestamp and add result
        List<Long> timeStamps2 = new LinkedList<>();
        List<List<Object>> result2 = new LinkedList<>();
        for(int i = 0; i < size2; i++) {
            List<Object> line = new LinkedList<>();
            line.add(i + 1);
            line.add(("test2" + (i + 1)).getBytes());
            timeStamps2.add((long) (i * 50 + 251));
            result2.add(line);
        }
        QueryExecuteDataSet dataSet2 = new SimpleQueryExecuteDataSet(
                columnTypeList2, columnNameList2, timeStamps2, result2
        );

        List<String> path2 = new LinkedList<>();
        path2.add("root.a.a.b");
        path2.add("root.a.a.c");
        QueryDataPlan plan2 = new QueryDataPlan(path2, 251, size2 * 50 + 251);

        List<QueryExecuteDataSet> set2 = new LinkedList<>();
        set2.add(dataSet2);
        PlanExecuteResult per2 = new
                QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan2, set2);
        planExecuteResults.add(per2);
        r.setPlanExecuteResults(planExecuteResults);

        // combine result
        QueryDataCombineResult res = (QueryDataCombineResult) c.combineResult(r);

        List<DataType> dt = res.getResp().dataTypeList;
        List<String> dn = res.getResp().getPaths();

        QueryDataSet q = res.getResp().getQueryDataSet();
        byte[] timeStampByte = q.getTimestamps();
        long[] timeStamp = ByteUtils.getLongArrayFromByteArray(timeStampByte);
        List<ByteBuffer> val = q.getValuesList();
        List<ByteBuffer> bit = q.getBitmapList();
        assertEquals(timeStamp.length, size1 + size2);
        assertEquals(val.size(), size1 + size2);
        assertEquals(bit.size(), size1 + size2);

        for(int i = 0; i < timeStamp.length; i++) {
            long time = timeStamp[i];
            ByteBuffer bitmap = q.getBitmapList().get(i);
            ByteBuffer value = q.getValuesList().get(i);
            byte[] bytes = new byte[bitmap.remaining()];
            int size = bytes.length * 8;
            Bitmap b = new Bitmap(size, bytes);
            bitmap.get(bytes, 0, bytes.length);
            for (int j = 0; j < size; j++){
                if (b.get(j)) {
                    if(dt.get(j) == DataType.BINARY) {
                        //assertEquals(dn.get(j), "c");
                        //only line 1 is integer，else are Strings
                        String queryResult = new String((byte[]) ByteUtils.getValueByDataType(value, dt.get(j)));
                        if(time % 100 == 0){
                            assertEquals(dn.get(j), "a");
                            assertEquals("test" + ((time + 100) / 100), queryResult);
                        } else {
                            assertEquals(dn.get(j), "c");
                            assertEquals("test2" + ((time - 201) / 50) , queryResult);
                        }
                    } else {
                        assertEquals(dn.get(j),"b");
                        long queryResult = (long)(Integer)ByteUtils.getValueByDataType(value, dt.get(j));
                        if(time % 100 == 0){
                            assertEquals(time, queryResult);
                        } else {
                            assertEquals((time - 201) / 50 , queryResult);
                        }
                    }
                }
            }
        }
    }

}
