package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.List;

public class InsertOperator extends Operator {

    private String prefixPath;
    private List<String> paths;
    private long[] times;
    private Object[] values;
    private List<DataType> types;

    public InsertOperator() {
        this.operatorType = OperatorType.INSERT;
        paths = new ArrayList<>();
        types = new ArrayList<>();
    }

    public String getPrefixPath() {
        return prefixPath;
    }

    public void setPrefixPath(String prefixPath) {
        this.prefixPath = prefixPath;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPath(String path) {
        this.paths.add(prefixPath + SQLConstant.DOT + path);
    }

    public long[] getTimes() {
        return times;
    }

    public void setTimes(long[] times) {
        this.times = times;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[][] values) {
        this.values = values;
    }

    public List<DataType> getTypes() {
        return types;
    }

    public void setTypes(List<DataType> types) {
        this.types = types;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        InsertNonAlignedColumnRecordsReq req = SortUtils.sortAndBuildInsertReq(
                sessionId,
                paths,
                times,
                values,
                types,
                null
        );
        return new ExecuteSqlResp(worker.insertNonAlignedColumnRecords(req), SqlType.Insert);
    }
}
