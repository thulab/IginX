package cn.edu.tsinghua.iginx.influxdb;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.influxdb.query.entity.InfluxDBQueryExecuteDataSet;
import cn.edu.tsinghua.iginx.metadata.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.IStorageEngine;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
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
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;
import static com.influxdb.client.domain.WritePrecision.MS;

public class InfluxDBPlanExecutor implements IStorageEngine {

	private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

	private static final WritePrecision WRITE_PRECISION = MS;

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	private static final String QUERY_DATA_WITH_TAG = "from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\" and r._field == \"%s\" and r.t == \"%s\")";

	private static final String QUERY_DATA_WITHOUT_TAG = "from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\" and r._field == \"%s\")";

	private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\" AND t=\"%s\"";

	private static final String MAX_WITH_TAG = QUERY_DATA_WITH_TAG + " |> max()";

	private static final String MIN_WITH_TAG = QUERY_DATA_WITH_TAG + " |> min()";

	private static final String FIRST_WITH_TAG = QUERY_DATA_WITH_TAG + " |> first()";

	private static final String LAST_WITH_TAG = QUERY_DATA_WITH_TAG + " |> last()";

	private static final String COUNT_WITH_TAG = QUERY_DATA_WITH_TAG + " |> count()";

	private static final String SUM_WITH_TAG = QUERY_DATA_WITH_TAG + " |> sum()";

	private static final String MAX_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> max()";

	private static final String MIN_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> min()";

	private static final String FIRST_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> first()";

	private static final String LAST_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> last()";

	private static final String COUNT_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> count()";

	private static final String SUM_WITHOUT_TAG = QUERY_DATA_WITHOUT_TAG + " |> sum()";

	private Map<Long, InfluxDBClient> storageEngineIdToClient;

	private void createConnection(StorageEngineMeta storageEngineMeta) {
		if (storageEngineMeta.getDbType() != StorageEngine.InfluxDB) {
			logger.warn("unexpected database: " + storageEngineMeta.getDbType());
			return;
		}
		Map<String, String> extraParams = storageEngineMeta.getExtraParams();
		String url = extraParams.getOrDefault("url", "http://localhost:8086/");
		InfluxDBClient client = InfluxDBClientFactory.create(url, ConfigDescriptor.getInstance().getConfig().getInfluxDBToken().toCharArray());
		storageEngineIdToClient.put(storageEngineMeta.getId(), client);
	}

	public InfluxDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
		storageEngineIdToClient = new ConcurrentHashMap<>();
		for (StorageEngineMeta storageEngineMeta : storageEngineMetaList) {
			createConnection(storageEngineMeta);
		}
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
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			Map<String, String> tags = new HashMap<>();
			if (elements.length > 3) {
				tags.put("t", plan.getPath(i).substring(plan.getPath(i).indexOf(".", plan.getPath(i).indexOf(".") + 1) + 1, plan.getPath(i).lastIndexOf(".")));
			}

			Bucket bucket = client.getBucketsApi()
					.findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
					.filter(b -> b.getName().equals(bucketName))
					.findFirst()
					.orElseThrow(IllegalStateException::new);
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
			String[] elements = plan.getPath(i).split("\\.");
			String bucketName = elements[0];
			Bucket bucket = client.getBucketsApi()
					.findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
					.filter(b -> b.getName().equals(bucketName))
					.findFirst()
					.orElseThrow(IllegalStateException::new);
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
					String measurement = elements[1];
					String field = elements[elements.length - 1];
					Map<String, String> tags = new HashMap<>();
					if (elements.length > 3) {
						tags.put("t", plan.getPath(j).substring(plan.getPath(j).indexOf(".", plan.getPath(j).indexOf(".") + 1) + 1, plan.getPath(j).lastIndexOf(".")));
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

		List<QueryExecuteDataSet> dataSets = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			// TODO 处理时区
			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						QUERY_DATA_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						QUERY_DATA_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			dataSets.addAll(tables.stream().map(x -> new InfluxDBQueryExecuteDataSet(bucketName, x)).collect(Collectors.toList()));
		}

		return new QueryDataPlanExecuteResult(SUCCESS, plan, dataSets);
	}

	@Override
	public NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
		return new NonDataPlanExecuteResult(SUCCESS, plan);
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
			String bucketName = elements[0];
			String measurement = elements[1];
			String value = plan.getPath(i).substring(plan.getPath(i).indexOf(".", plan.getPath(i).indexOf(".") + 1) + 1, plan.getPath(i).lastIndexOf("."));
			String field = elements[elements.length - 1];

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
	public NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
		InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
		Organization organization = client.getOrganizationsApi()
				.findOrganizations().stream()
				.filter(o -> o.getName().equals(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()))
				.findFirst()
				.orElseThrow(IllegalStateException::new);
		client.getBucketsApi().createBucket(plan.getDatabaseName(), organization);
		return new NonDataPlanExecuteResult(SUCCESS, plan);
	}

	@Override
	public NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
		InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
		Bucket bucket = client.getBucketsApi()
				.findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
				.filter(b -> b.getName().equals(plan.getDatabaseName()))
				.findFirst()
				.orElseThrow(IllegalStateException::new);
		client.getBucketsApi().deleteBucket(bucket);
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Long> counts = new ArrayList<>();
		List<Object> sums = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> countTables;
			List<FluxTable> sumTables;
			if (value != null) {
				countTables = client.getQueryApi().query(String.format(
						COUNT_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
				sumTables = client.getQueryApi().query(String.format(
						SUM_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				countTables = client.getQueryApi().query(String.format(
						COUNT_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
				sumTables = client.getQueryApi().query(String.format(
						SUM_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			}

			if (!countTables.isEmpty() && !sumTables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(sumTables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				counts.add((Long) countTables.get(0).getRecords().get(0).getValue());
				sums.add(sumTables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(LONG);
				counts.add(0L);
				sums.add(0L);
			}
		}

		AvgAggregateQueryPlanExecuteResult result = new AvgAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						COUNT_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						COUNT_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(LONG);
				values.add(0L);
			}
		}

		StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						SUM_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						SUM_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(LONG);
				values.add(0L);
			}
		}

		StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						FIRST_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						FIRST_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				timestamps.add(tables.get(0).getRecords().get(0).getTime().toEpochMilli());
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(BINARY);
				timestamps.add(-1L);
				values.add("null".getBytes());
			}
		}

		SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						LAST_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						LAST_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				timestamps.add(tables.get(0).getRecords().get(0).getTime().toEpochMilli());
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(BINARY);
				timestamps.add(-1L);
				values.add("null".getBytes());
			}
		}

		SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						MAX_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						MAX_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				timestamps.add(tables.get(0).getRecords().get(0).getTime().toEpochMilli());
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(BINARY);
				timestamps.add(-1L);
				values.add("null".getBytes());
			}
		}

		SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
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

		List<DataType> dataTypeList = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String bucketName = elements[0];
			String measurement = elements[1];
			String field = elements[elements.length - 1];
			String value = null;
			if (elements.length > 3) {
				value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			}

			List<FluxTable> tables;
			if (value != null) {
				tables = client.getQueryApi().query(String.format(
						MIN_WITH_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field,
						value
				), organization.getId());
			} else {
				tables = client.getQueryApi().query(String.format(
						MIN_WITHOUT_TAG,
						bucketName,
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
						ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
						measurement,
						field
				), organization.getId());
			}

			if (!tables.isEmpty()) {
				dataTypeList.add(fromInfluxDB(tables.get(0).getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
				timestamps.add(tables.get(0).getRecords().get(0).getTime().toEpochMilli());
				values.add(tables.get(0).getRecords().get(0).getValue());
			} else {
				dataTypeList.add(BINARY);
				timestamps.add(-1L);
				values.add("null".getBytes());
			}
		}

		SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
		result.setPaths(plan.getPaths());
		result.setTimes(timestamps);
		result.setDataTypes(dataTypeList);
		result.setValues(values);

		return result;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan) {
		return null;
	}

	@Override
	public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan) {
		return null;
	}

	@Override
	public StorageEngineChangeHook getStorageEngineChangeHook() {
		return null;
	}
}
