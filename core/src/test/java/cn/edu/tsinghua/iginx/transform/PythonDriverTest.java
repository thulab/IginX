package cn.edu.tsinghua.iginx.transform;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.data.ArrowWriter;
import cn.edu.tsinghua.iginx.transform.data.LogWriter;
import cn.edu.tsinghua.iginx.transform.driver.PythonDriver;
import cn.edu.tsinghua.iginx.transform.driver.IPCWorker;
import cn.edu.tsinghua.iginx.transform.exception.TransformException;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class PythonDriverTest {

    @Test
    public void test() throws InterruptedException, TransformException {
        PythonDriver driver = PythonDriver.getInstance();
        PythonTask task = new PythonTask(TaskType.Python, DataFlowType.Stream, Long.MAX_VALUE,
            "RowSumTransformer");

        IPCWorker IPCWorker = driver.createWorker(task, new LogWriter());
        IPCWorker.start();

//        ArrowReader reader = new ArrowReader();

        for (int i = 0; i < 3; i++) {
            VectorSchemaRoot root = prepareData();
//        Schema schema = root.getSchema();
//        schema.getFields().forEach(field -> System.out.println(field.getType().toString()));
            ArrowWriter writer = new ArrowWriter(IPCWorker.getPyPort());
            writer.writeVector(root);
//        worker.writeMsg(root);
            Thread.sleep(50);
        }


        Thread.sleep(5000);

        IPCWorker.close();
    }

    private VectorSchemaRoot prepareData() {
        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        BigIntVector bigIntVector = new BigIntVector(GlobalConstant.KEY_NAME, allocator);
        IntVector intVector1 = new IntVector("root.value1", allocator);
        IntVector intVector2 = new IntVector("root.value2", allocator);
//        BitVector bitVector = new BitVector("root.value3", allocator);
//        Float4Vector float4Vector = new Float4Vector("root.value4", allocator);
//        Float8Vector float8Vector = new Float8Vector("root.value5", allocator);
//        VarCharVector varCharVector = new VarCharVector("root.value6", allocator);

        for (int i = 0; i < 10; i++) {
            bigIntVector.setSafe(i, i);
            intVector1.setSafe(i, i);
            intVector2.setSafe(i, i + 1);
        }

        bigIntVector.setValueCount(10);
        intVector1.setValueCount(10);
        intVector2.setValueCount(10);

        List<Field> fields = Arrays.asList(bigIntVector.getField(), intVector1.getField(), intVector2.getField());
        List<FieldVector> vectors = Arrays.asList(bigIntVector, intVector1, intVector2);

//        List<Field> fields = Arrays.asList(bigIntVector.getField(), intVector1.getField(), intVector2.getField(), bitVector.getField(), float4Vector.getField(), float8Vector.getField(), varCharVector.getField());
//        List<FieldVector> vectors = Arrays.asList(bigIntVector, intVector1, intVector2, bitVector, float4Vector, float8Vector, varCharVector);
        VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
        return root;
    }
}
