package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class SessionExecuteSqlResult {

    private SqlType sqlType;
    private AggregateType aggregateType;

    private long[] timestamps;
    private List<String> paths;
    private List<List<Object>> values;
    private Object[] AggregateValues;
    private int replicaNum;

    public SessionExecuteSqlResult(ExecuteSqlResp resp) {
        this.sqlType = resp.getType();
        switch (resp.getType()) {
            case Insert:
            case Delete:
            case AddStorageEngines:
                break;
            case AggregateQuery:
                this.paths = resp.getPaths();
                if (resp.timestamps != null) {
                    this.timestamps = getLongArrayFromByteBuffer(resp.timestamps);
                }
                this.AggregateValues = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
                this.aggregateType = resp.aggregateType;
                break;
            case SimpleQuery:
            case DownsampleQuery:
            case ValueFilterQuery:
                this.paths = resp.getPaths();
                if (resp.queryDataSet.timestamps != null) {
                    this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
                }
                parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
                if (resp.getType() == SqlType.DownsampleQuery) {
                    this.aggregateType = resp.aggregateType;
                }
        }
    }

    private void parseValues(List<DataType> dataTypeList, List<ByteBuffer> valuesList, List<ByteBuffer> bitmapList) {
        values = new ArrayList<>();
        for (int i = 0; i < valuesList.size(); i++) {
            List<Object> tempValues = new ArrayList<>();
            ByteBuffer valuesBuffer = valuesList.get(i);
            ByteBuffer bitmapBuffer = bitmapList.get(i);
            Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
            for (int j = 0; j < dataTypeList.size(); j++) {
                if (bitmap.get(j)) {
                    tempValues.add(getValueFromByteBufferByDataType(valuesBuffer, dataTypeList.get(j)));
                } else {
                    tempValues.add(null);
                }
            }
            values.add(tempValues);
        }
    }

    public void print() {
        System.out.printf("Start to Print %s ResultSets:\n", sqlType.toString());

        if (sqlType == SqlType.SimpleQuery || sqlType == SqlType.DownsampleQuery || sqlType == SqlType.ValueFilterQuery) {
            System.out.print("Time\t");
            for (String path : paths) {
                System.out.print(path + "\t");
            }
            System.out.println();

            for (int i = 0; i < timestamps.length; i++) {
                System.out.print(timestamps[i] + "\t");
                for (int j = 0; j < paths.size(); j++) {
                    if (values.get(i).get(j) instanceof byte[]) {
                        System.out.print(new String((byte[]) values.get(i).get(j)) + "\t");
                    } else {
                        System.out.print(values.get(i).get(j) + "\t");
                    }
                }
                System.out.println();
            }
        } else if (sqlType == SqlType.AggregateQuery) {
            System.out.println("Start to Print ResultSets:");
            if (timestamps == null) {
                for (String path : paths) {
                    System.out.print(aggregateType.toString() + "(" + path + ")\t");
                }
                System.out.println();
                for (Object value : AggregateValues) {
                    if (value instanceof byte[]) {
                        System.out.print(new String((byte[]) value) + "\t");
                    } else {
                        System.out.print(value + "\t");
                    }
                }
                System.out.println();
            } else {
                for (int i = 0; i < timestamps.length; i++) {
                    System.out.print("Time\t");
                    System.out.print(aggregateType.toString() + "(" + paths.get(i) + ")\t");
                    System.out.println();
                    System.out.print(timestamps[i] + "\t");
                    if (AggregateValues[i] instanceof byte[]) {
                        System.out.print(new String((byte[]) AggregateValues[i]) + "\t");
                    } else {
                        System.out.print(AggregateValues[i] + "\t");
                    }
                    System.out.println();
                }
            }
        }
        System.out.println("Printing ResultSets Finished.");
    }

    public boolean needPrint() {
        return sqlType == SqlType.SimpleQuery ||
                sqlType == SqlType.AggregateQuery ||
                sqlType == SqlType.DownsampleQuery ||
                sqlType == SqlType.ValueFilterQuery;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public long[] getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(long[] timestamps) {
        this.timestamps = timestamps;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void setValues(List<List<Object>> values) {
        this.values = values;
    }

    public Object[] getAggregateValues() {
        return AggregateValues;
    }

    public void setAggregateValues(Object[] aggregateValues) {
        AggregateValues = aggregateValues;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }
}
