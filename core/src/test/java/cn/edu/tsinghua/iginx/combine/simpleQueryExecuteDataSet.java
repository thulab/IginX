
package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;


public class simpleQueryExecuteDataSet implements QueryExecuteDataSet {

    List<DataType> columnTypes;
    List<String> columnNames;
    int index;
    int num;
    List<Long> timeStamps;
    List<List<Object>> result;


    public simpleQueryExecuteDataSet(List<DataType> columnTypes, List<String> columnNames,
                                        List<Long> timeStamps, List<List<Object>> result) {
        this.columnTypes = columnTypes;
        this.columnNames = columnNames;
        this.timeStamps = timeStamps;
        this.index = 0;
        this.num = timeStamps.size();
        this.result = result;
    }

    @Override
    public boolean hasNext() throws ExecutionException {
        if (index < num) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public List<DataType> getColumnTypes() throws ExecutionException {
        return columnTypes;
    }

    @Override
    public List<String> getColumnNames() throws ExecutionException {
        return columnNames;
    }

    @Override
    public RowRecord next() throws ExecutionException {
        RowRecord rowRecord = new RowRecord(timeStamps.get(index), result.get(index));
        System.out.println("index="+index+" ,rowRecordtime="+timeStamps.get(index)+" result="+result.get(index));
        index += 1;
        return rowRecord;
    }

    @Override
    public void close() throws ExecutionException {

    }
}
