package cn.edu.tsinghua.iginx.influxdb;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBQueryExecuteDataSet;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.IStorageEngine;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.DOUBLE;
import static com.influxdb.client.domain.WritePrecision.MS;

public class InfluxDBPlanExecutor implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

    private static final WritePrecision WRITE_PRECISION = MS;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final String QUERY_DATA = "from(bucket:\"%s\") |> range(start: %s, stop: %s)";

    private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\" AND t=\"%s\"";

    private final Map<Long, InfluxDBClient> storageEngineIdToClient;

    public InfluxDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        storageEngineIdToClient = new ConcurrentHashMap<>();
        for (StorageEngineMeta storageEngineMeta : storageEngineMetaList) {
            if (!createConnection(storageEngineMeta)) {
                System.exit(1);
            }
        }
    }

    public static boolean testConnection(StorageEngineMeta storageEngineMeta) {
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String url = extraParams.get("url");
        try {
            InfluxDBClient client = InfluxDBClientFactory.create(url, ConfigDescriptor.getInstance().getConfig().getInfluxDBToken().toCharArray());
            client.close();
        } catch (Exception e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    private boolean createConnection(StorageEngineMeta storageEngineMeta) {
        if (storageEngineMeta.getDbType() != StorageEngine.InfluxDB) {
            logger.warn("unexpected database: " + storageEngineMeta.getDbType());
            return false;
        }
        if (!testConnection(storageEngineMeta)) {
            logger.error("cannot connect to " + storageEngineMeta.toString());
            return false;
        }
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String url = extraParams.getOrDefault("url", "http://localhost:8086/");
        InfluxDBClient client = InfluxDBClientFactory.create(url, ConfigDescriptor.getInstance().getConfig().getInfluxDBToken().toCharArray());
        storageEngineIdToClient.put(storageEngineMeta.getId(), client);
        return true;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        Map<Bucket, List<Point>> bucketToPoints = new HashMap<>();
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String[] elements = plan.getPath(i).split("\\.");
            String bucketName = plan.getStorageUnit().getId();
            String measurement = elements[0];
            String field = elements[elements.length - 1];
            Map<String, String> tags = new HashMap<>();
            if (elements.length > 2) {
                tags.put("t", plan.getPath(i).substring(plan.getPath(i).indexOf(".") + 1, plan.getPath(i).lastIndexOf(".")));
            }

            List<Bucket> bucketList = client.getBucketsApi()
                    .findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
                    .filter(b -> b.getName().equals(bucketName))
                    .collect(Collectors.toList());
            Bucket bucket;
            if (bucketList.isEmpty()) {
                bucket = client.getBucketsApi().createBucket(bucketName, organization);
            } else {
                bucket = bucketList.get(0);
            }
            bucketToPoints.putIfAbsent(bucket, new ArrayList<>());

            Object[] values = (Object[]) (plan.getValuesList()[i]);
            Bitmap bitmap = plan.getBitmap(i);
            int k = 0;
            for (int j = 0; j < plan.getTimestamps().length; j++) {
                // TODO 增加处理精度
                if (bitmap.get(j)) {
                    switch (plan.getDataType(i)) {
                        case BOOLEAN:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (boolean) values[k]).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        case INTEGER:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (int) values[k]).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        case LONG:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (long) values[k]).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        case FLOAT:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (float) values[k]).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        case DOUBLE:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (double) values[k]).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        case BINARY:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, new String((byte[]) values[k])).time(plan.getTimestamp(j), WRITE_PRECISION));
                            break;
                        default:
                            throw new UnsupportedDataTypeException(plan.getDataType(i).toString());
                    }
                    k++;
                }
            }
        }

        for (Map.Entry<Bucket, List<Point>> entry : bucketToPoints.entrySet()) {
            if (plan.isSync()) {
                client.getWriteApiBlocking().writePoints(entry.getKey().getId(), organization.getId(), entry.getValue());
            } else {
                client.getWriteApi().writePoints(entry.getKey().getId(), organization.getId(), entry.getValue());
            }
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        Map<String, Bucket> pathToBucket = new HashMap<>();
        Map<Bucket, List<Point>> bucketToPoints = new HashMap<>();
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String bucketName = plan.getStorageUnit().getId();
            List<Bucket> bucketList = client.getBucketsApi()
                    .findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
                    .filter(b -> b.getName().equals(bucketName))
                    .collect(Collectors.toList());
            Bucket bucket;
            if (bucketList.isEmpty()) {
                bucket = client.getBucketsApi().createBucket(bucketName, organization);
            } else {
                bucket = bucketList.get(0);
            }
            pathToBucket.put(plan.getPath(i), bucket);
            bucketToPoints.putIfAbsent(bucket, new ArrayList<>());
        }

        for (int i = 0; i < plan.getTimestamps().length; i++) {
            Object[] values = (Object[]) (plan.getValuesList()[i]);
            Bitmap bitmap = plan.getBitmap(i);
            int k = 0;
            for (int j = 0; j < plan.getPathsNum(); j++) {
                if (bitmap.get(j)) {
                    String[] elements = plan.getPath(j).split("\\.");
                    String measurement = elements[0];
                    String field = elements[elements.length - 1];
                    Map<String, String> tags = new HashMap<>();
                    if (elements.length > 2) {
                        tags.put("t", plan.getPath(j).substring(plan.getPath(j).indexOf(".") + 1, plan.getPath(j).lastIndexOf(".")));
                    }

                    Bucket bucket = pathToBucket.get(plan.getPath(j));
                    switch (plan.getDataType(j)) {
                        case BOOLEAN:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (boolean) values[k]).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        case INTEGER:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (int) values[k]).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        case LONG:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (long) values[k]).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        case FLOAT:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (float) values[k]).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        case DOUBLE:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (double) values[k]).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        case BINARY:
                            bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, new String((byte[]) values[k])).time(plan.getTimestamp(i), WRITE_PRECISION));
                            break;
                        default:
                            throw new UnsupportedDataTypeException(plan.getDataType(i).toString());
                    }
                    k++;
                }
            }
        }

        for (Map.Entry<Bucket, List<Point>> entry : bucketToPoints.entrySet()) {
            if (plan.isSync()) {
                client.getWriteApiBlocking().writePoints(entry.getKey().getId(), organization.getId(), entry.getValue());
            } else {
                client.getWriteApi().writePoints(entry.getKey().getId(), organization.getId(), entry.getValue());
            }
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new QueryDataPlanExecuteResult(SUCCESS, plan, null);
        }

        List<QueryExecuteDataSet> dataSets = new ArrayList<>();
        for (String path : plan.getPaths()) {
            // TODO 处理时区
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
            dataSets.addAll(tables.stream().map(InfluxDBQueryExecuteDataSet::new).collect(Collectors.toList()));
        }

        return new QueryDataPlanExecuteResult(SUCCESS, plan, dataSets);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        for (int i = 0; i < plan.getPathsNum(); i++) {
            String[] elements = plan.getPath(i).split("\\.");
            String bucketName = plan.getStorageUnit().getId();
            String measurement = elements[0];
            String field = elements[elements.length - 1];
            String value = null;
            if (elements.length > 2) {
                value = plan.getPath(i).substring(plan.getPath(i).indexOf(".") + 1, plan.getPath(i).lastIndexOf("."));
            }

            Bucket bucket = client.getBucketsApi()
                    .findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
                    .filter(b -> b.getName().equals(bucketName))
                    .findFirst()
                    .orElseThrow(IllegalStateException::new);

            // TODO 当前版本的 InfluxDB 无法指定 _field
            client.getDeleteApi().delete(
                    OffsetDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")),
                    OffsetDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")),
                    String.format(DELETE_DATA, measurement, field, value),
                    bucket,
                    organization
            );
        }

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new AvgAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        List<Object> sums = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FluxTable> countTables;
            List<FluxTable> sumTables;
            try {
                countTables = client.getQueryApi().query(
                        generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime()) + " |> count()",
                        organization.getId()
                );
                sumTables = client.getQueryApi().query(
                        generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime()) + " |> sum()",
                        organization.getId()
                );
            } catch (Exception e) {
                if (e.getMessage().contains("unsupported input type for sum aggregate")) {
                    logger.error("unsupported input type for sum aggregate");
                    continue;
                } else {
                    return new AvgAggregateQueryPlanExecuteResult(FAILURE, plan);
                }
            }

            for (int i = 0; i < countTables.size(); i++) {
                paths.add(String.format("%s.%s.%s",
                        countTables.get(i).getRecords().get(0).getMeasurement(),
                        countTables.get(i).getRecords().get(0).getValueByKey("t"),
                        countTables.get(i).getRecords().get(0).getField()
                ));
                DataType dataType = fromInfluxDB(sumTables.get(i).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
                if (dataType == DataType.LONG) {
                    dataTypeList.add(DOUBLE);
                    Long value = (Long) sumTables.get(i).getRecords().get(0).getValue();
                    if (value == null) {
                        sums.add(null);
                    } else {
                        sums.add(value.doubleValue());
                    }
                } else {
                    dataTypeList.add(dataType);
                    sums.add(sumTables.get(i).getRecords().get(0).getValue());
                }
                counts.add((Long) countTables.get(i).getRecords().get(0).getValue());
            }
        }

        AvgAggregateQueryPlanExecuteResult result = new AvgAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setDataTypes(dataTypeList);
        result.setCounts(counts);
        result.setSums(sums);

        return result;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables = client.getQueryApi().query(statement + " |> count()", organization.getId());
            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                dataTypeList.add(fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
                values.add(table.getRecords().get(0).getValue());
            }
        }

        StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables;
            try {
                tables = client.getQueryApi().query(statement + " |> sum()", organization.getId());
            } catch (Exception e) {
                if (e.getMessage().contains("unsupported input type for sum aggregate")) {
                    continue;
                } else {
                    return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, plan);
                }
            }

            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
                if (dataType == DataType.LONG) {
                    dataTypeList.add(DOUBLE);
                    Long value = (Long) table.getRecords().get(0).getValue();
                    if (value == null) {
                        values.add(null);
                    } else {
                        values.add(value.doubleValue());
                    }
                } else {
                    dataTypeList.add(dataType);
                    values.add(table.getRecords().get(0).getValue());
                }
            }
        }

        StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables = client.getQueryApi().query(statement + " |> first()", organization.getId());
            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
                dataTypeList.add(dataType);
                timestamps.add(table.getRecords().get(0).getTime().toEpochMilli());
                if (dataType != BINARY) {
                    values.add(table.getRecords().get(0).getValue());
                } else {
                    values.add(((String) table.getRecords().get(0).getValue()).getBytes());
                }
            }
        }

        SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setTimes(timestamps);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables = client.getQueryApi().query(statement + " |> last()", organization.getId());
            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
                dataTypeList.add(dataType);
                timestamps.add(table.getRecords().get(0).getTime().toEpochMilli());
                if (dataType != BINARY) {
                    values.add(table.getRecords().get(0).getValue());
                } else {
                    values.add(((String) table.getRecords().get(0).getValue()).getBytes());
                }
            }
        }

        SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setTimes(timestamps);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables;
            try {
                tables = client.getQueryApi().query(statement + " |> max()", organization.getId());
            } catch (Exception e) {
                // TODO 字符串类型不支持 Max 和 Min
                if (e.getMessage().contains("panic: unsupported for aggregate max")) {
                    continue;
                } else {
                    return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
                }
            }

            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                dataTypeList.add(fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
                values.add(table.getRecords().get(0).getValue());
                timestamps.add(table.getRecords().get(0).getTime().toEpochMilli());
            }
        }

        SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setTimes(timestamps);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        }

        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables;
            try {
                tables = client.getQueryApi().query(statement + " |> min()", organization.getId());
            } catch (Exception e) {
                if (e.getMessage().contains("panic: unsupported for aggregate min")) {
                    continue;
                } else {
                    return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
                }
            }

            for (FluxTable table : tables) {
                paths.add(String.format("%s.%s.%s",
                        table.getRecords().get(0).getMeasurement(),
                        table.getRecords().get(0).getValueByKey("t"),
                        table.getRecords().get(0).getField()
                ));
                dataTypeList.add(fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
                values.add(table.getRecords().get(0).getValue());
                timestamps.add(table.getRecords().get(0).getTime().toEpochMilli());
            }
        }

        SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setTimes(timestamps);
        result.setDataTypes(dataTypeList);
        result.setValues(values);

        return result;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan) {
        return syncExecuteDownsampleQueryPlan(plan);
    }

    @Override
    public ValueFilterQueryPlanExecuteResult syncExecuteValueFilterQueryPlan(ValueFilterQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new ValueFilterQueryPlanExecuteResult(SUCCESS, plan, new ArrayList<>());
        }

        List<QueryExecuteDataSet> dataSets = new ArrayList<>();
        for (String path : plan.getPaths()) {
            // TODO 处理时区
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            List<FluxTable> tables = client.getQueryApi().query(statement, organization.getId());
            dataSets.addAll(tables.stream().map(InfluxDBQueryExecuteDataSet::new).collect(Collectors.toList()));
        }

        return new ValueFilterQueryPlanExecuteResult(SUCCESS, plan, dataSets);
    }

    @Override
    public ShowColumnsPlanExecuteResult syncExecuteShowColumnsPlan(ShowColumnsPlan plan) {
        return null;
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            if (before == null && after != null) {
                logger.info("a new influxdb engine added: " + after.getIp() + ":" + after.getPort());
                createConnection(after);
            }
            // TODO: 考虑结点删除等情况
        };
    }

    private DownsampleQueryPlanExecuteResult syncExecuteDownsampleQueryPlan(DownsampleQueryPlan plan) {
        InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(plan.getStorageUnit().getId()) == null) {
            logger.error("storage engine {} doesn't exist", plan.getStorageUnit().getId());
            return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, new ArrayList<>());
        }

        List<QueryExecuteDataSet> dataSets = new ArrayList<>();
        for (String path : plan.getPaths()) {
            String statement = generateQueryStatement(plan.getStorageUnit().getId(), path, plan.getStartTime(), plan.getEndTime());
            switch (plan.getIginxPlanType()) {
                case DOWNSAMPLE_AVG:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: mean, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_SUM:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: sum, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_COUNT:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: count, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_MAX:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: max, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_MIN:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: min, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_FIRST:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: first, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                case DOWNSAMPLE_LAST:
                    statement += String.format(" |> aggregateWindow(every: %sms, fn: last, timeSrc: \"_start\")", plan.getPrecision());
                    break;
                default:
                    throw new UnsupportedOperationException(plan.getIginxPlanType().toString());
            }

            List<FluxTable> tables;
            try {
                tables = client.getQueryApi().query(statement, organization.getId());
            } catch (Exception e) {
                if (e.getMessage().contains("panic: unsupported for aggregate max")
                        || e.getMessage().contains("panic: unsupported for aggregate min")
                        || e.getMessage().contains("unsupported input type for sum aggregate")
                        || e.getMessage().contains("unsupported input type for mean aggregate")) {
                    continue;
                } else {
                    return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
                }
            }

            dataSets.addAll(tables.stream().map(InfluxDBQueryExecuteDataSet::new).collect(Collectors.toList()));
        }

        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, dataSets);
    }

    private String generateQueryStatement(String bucketName, String path, long startTime, long endTime) {
        String statement = String.format(
                QUERY_DATA,
                bucketName,
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTime), ZoneId.of("UTC")).format(FORMATTER),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTime), ZoneId.of("UTC")).format(FORMATTER)
        );
        if (!path.equals("*")) {
            StringBuilder filterStr = new StringBuilder(" |> filter(fn: (r) => ");
            filterStr.append(path.substring(0, path.indexOf('.')).equals("*") ? "r._measurement =~ /.*/" : "r._measurement == \"" + path.substring(0, path.indexOf('.')) + "\"");
            filterStr.append(" and ");
            filterStr.append(path.substring(path.lastIndexOf('.') + 1).equals("*") ? "r._field =~ /.*/" : "r._field == \"" + path.substring(path.lastIndexOf('.') + 1) + "\"");
            if (path.split("\\.").length > 2) {
                filterStr.append(" and ");
                String tagString = path.substring(path.indexOf(".") + 1, path.lastIndexOf("."));
                if (tagString.contains("*")) {
                    StringBuilder value = new StringBuilder("/");
                    for (Character character : tagString.toCharArray()) {
                        if (character.equals('.')) {
                            value.append("\\\\.");
                        } else if (character.equals('*')) {
                            value.append(".*");
                        } else {
                            value.append(character);
                        }
                    }
                    value.append("/");
                    tagString = value.toString();
                    filterStr.append("r.t =~ /");
                    filterStr.append(tagString);
                    filterStr.append("/");
                } else {
                    filterStr.append("r.t == \"");
                    filterStr.append(tagString);
                    filterStr.append("\"");
                }
            }
            filterStr.append(")");
            statement += filterStr;
        }
        return statement;
    }
}
