package cn.edu.tsinghua.iginx.influxdb.query.entity;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.DOUBLE;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;

public class InfluxDBQueryExecuteDataSet implements QueryExecuteDataSet {

    private final FluxTable table;

    private int index;

    private final boolean transferToDouble;

    public InfluxDBQueryExecuteDataSet(FluxTable table) {
        this.table = table;
        this.index = 0;
        this.transferToDouble = false;
    }

    public InfluxDBQueryExecuteDataSet(FluxTable table, boolean transferToDouble) {
        this.table = table;
        this.index = 0;
        this.transferToDouble = transferToDouble;
    }

    @Override
    public List<String> getColumnNames() throws ExecutionException {
        List<String> columnNames = new ArrayList<>();
        columnNames.add("Time");
        if (table.getRecords().get(0).getValueByKey("t") == null) {
            columnNames.add(table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getField());
        } else {
            columnNames.add(table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getValueByKey("t") + "." + table.getRecords().get(0).getField());
        }
        return columnNames;
    }

    @Override
    public List<DataType> getColumnTypes() throws ExecutionException {
        List<DataType> columnTypes = new ArrayList<>();
        columnTypes.add(LONG);
        DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
        if (transferToDouble && dataType == LONG) {
            dataType = DOUBLE;
        }
        columnTypes.add(dataType);
        return columnTypes;
    }

    @Override
    public boolean hasNext() throws ExecutionException {
        return index < table.getRecords().size();
    }

    @Override
    public RowRecord next() throws ExecutionException {
        RowRecord rowRecord = new RowRecord(table.getRecords().get(index).getTime().toEpochMilli());
        Object value = table.getRecords().get(index).getValue();
        DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
        if (dataType == BINARY) {
            rowRecord.setFields(Collections.singletonList(((String) value).getBytes()));
        } else if (dataType == LONG && transferToDouble) {
            if (value == null) {
                rowRecord.setFields(Collections.singletonList(null));
            } else {
                rowRecord.setFields(Collections.singletonList(((Long) value).doubleValue()));
            }
        } else {
            rowRecord.setFields(Collections.singletonList(value));
        }
        index++;
        return rowRecord;
    }

    @Override
    public void close() throws ExecutionException {
        // TODO do nothing
    }
}
