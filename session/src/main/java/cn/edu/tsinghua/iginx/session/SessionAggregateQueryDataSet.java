package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;

public class SessionAggregateQueryDataSet {

    private final AggregateType type;

    private List<String> paths;

    private long[] keys;

    private final Object[] values;

    public SessionAggregateQueryDataSet(AggregateQueryResp resp, AggregateType type) {
        this.paths = resp.getPaths();
        if (resp.timestamps != null) {
            this.keys = getLongArrayFromByteBuffer(resp.timestamps);
        }
        this.values = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
        this.type = type;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public long[] getKeys() {
        return keys;
    }

    public Object[] getValues() {
        return values;
    }

    public void print() {
        System.out.println("Start to Print ResultSets:");
        if (keys == null) {
            for (String path : paths) {
                System.out.print(path + "\t");
            }
            System.out.println();
            for (Object value : values) {
                if (value instanceof byte[]) {
                    System.out.print(new String((byte[]) value) + "\t");
                } else {
                    System.out.print(value + "\t");
                }
            }
            System.out.println();
        } else {
            for (int i = 0; i < keys.length; i++) {
                System.out.print("Time\t");
                System.out.print(paths.get(i) + "\t");
                System.out.println();
                System.out.print(keys[i] + "\t");
                if (values[i] instanceof byte[]) {
                    System.out.print(new String((byte[]) values[i]) + "\t");
                } else {
                    System.out.print(values[i] + "\t");
                }
                System.out.println();
            }
        }
        System.out.println("Printing ResultSets Finished.");
    }
}
