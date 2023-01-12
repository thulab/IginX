package cn.edu.tsinghua.iginx.influxdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBSchema;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.util.*;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;

public class SchemaTransformer {

    public static Field toField(String bucket, FluxTable table) {
        FluxRecord record = table.getRecords().get(0);
        String measurement = record.getMeasurement();
        String field = record.getField();
        List<FluxColumn> columns = table.getColumns();
        columns = columns.subList(8, columns.size());
        List<Pair<String, String>> tagKVs = new ArrayList<>();
        for (FluxColumn column : columns) {
            String tagK = column.getLabel();
            String tagV = (String) record.getValueByKey(tagK);
            tagKVs.add(new Pair<>(tagK, tagV));
        }
        tagKVs.sort(Comparator.comparing(o -> o.k));
        DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());

        StringBuilder pathBuilder = new StringBuilder();
        pathBuilder.append(bucket);
        pathBuilder.append('.');
        pathBuilder.append(measurement);
        pathBuilder.append('.');
        pathBuilder.append(field);
        Map<String, String> tags = new HashMap<>();
        for (Pair<String, String> tagKV: tagKVs) {
            tags.put(tagKV.k, tagKV.v);
        }
        return new Field(pathBuilder.toString(), dataType, tags);
    }

    public static Pair<String, String> processPatternForQuery(String pattern, TagFilter tagFilter) { // 返回的是 bucket_name, query 的信息
        String[] parts = pattern.split("\\.", 3);
        int index = 0;
        String bucketName = parts[index++];
        if (index >= parts.length) {
            return new Pair<>(bucketName, "true");
        }
        StringBuilder queryBuilder = new StringBuilder("(");
        String measurementName = parts[index++];
        if (!measurementName.equals("*")) {
            queryBuilder.append(String.format("r._measurement ==\"%s\"", measurementName));
        }
        if (index < parts.length) {
            // 接着处理 field
            String field = parts[index];
            queryBuilder.append(" and r._field =~ /").append(InfluxDBSchema.transformField(field)).append("/");
        }
        queryBuilder.append(")");
        return new Pair<>(bucketName, queryBuilder.toString());
    }

}
