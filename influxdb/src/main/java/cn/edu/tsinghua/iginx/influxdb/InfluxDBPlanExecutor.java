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
import cn.edu.tsinghua.iginx.query.AbstractPlanExecutor;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;
import static com.influxdb.client.domain.WritePrecision.MS;

public class InfluxDBPlanExecutor extends AbstractPlanExecutor {

	private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

	private static final WritePrecision WRITE_PRECISION = MS;

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	private static final String QUERY_DATA = "from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\" and r._field == \"%s\" and r.tag == \"%s\")";

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
	protected NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
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
			String value = plan.getPath(i).substring(plan.getPath(i).indexOf(".", plan.getPath(i).indexOf(".") + 1) + 1, plan.getPath(i).lastIndexOf("."));
			String field = elements[elements.length - 1];
			Map<String, String> tags = new HashMap<>();
			tags.put("tag", value);

			Bucket bucket = client.getBucketsApi()
					.findBucketsByOrgName(ConfigDescriptor.getInstance().getConfig().getInfluxDBOrganizationName()).stream()
					.filter(b -> b.getName().equals(bucketName))
					.findFirst()
					.orElseThrow(IllegalStateException::new);
			bucketToPoints.putIfAbsent(bucket, new ArrayList<>());

			Object[] values = (Object[]) (plan.getValuesList()[i]);
			for (int j = 0; j < plan.getTimestamps().length; j++) {
				// TODO 增加处理精度
				switch (plan.getDataType(i)) {
					case BOOLEAN:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (boolean) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					case INTEGER:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (int) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					case LONG:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (long) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					case FLOAT:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (float) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					case DOUBLE:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (double) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					case BINARY:
						bucketToPoints.get(bucket).add(Point.measurement(measurement).addTags(tags).addField(field, (String) values[j]).time(plan.getTimestamp(j), WRITE_PRECISION));
						break;
					default:
						throw new UnsupportedDataTypeException(plan.getDataType(i).toString());
				}
			}
		}

		for (Map.Entry<Bucket, List<Point>> entry : bucketToPoints.entrySet()) {
			client.getWriteApi().writePoints(entry.getKey().getId(), organization.getId(), entry.getValue());
		}
		return new NonDataPlanExecuteResult(SUCCESS, plan);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
		return null;
	}

	@Override
	protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
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
			String value = path.substring(path.indexOf(".", path.indexOf(".") + 1) + 1, path.lastIndexOf("."));
			String field = elements[elements.length - 1];

			List<FluxTable> tables = client.getQueryApi().query(String.format(
					QUERY_DATA,
					bucketName,
					ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getStartTime()), ZoneId.of("UTC")).format(FORMATTER),
					ZonedDateTime.ofInstant(Instant.ofEpochMilli(plan.getEndTime() + 1), ZoneId.of("UTC")).format(FORMATTER),
					measurement,
					field,
					value
			), organization.getId());
			dataSets.addAll(tables.stream().map(x -> new InfluxDBQueryExecuteDataSet(bucketName, x)).collect(Collectors.toList()));
		}

		return new QueryDataPlanExecuteResult(SUCCESS, plan, dataSets);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
		return new NonDataPlanExecuteResult(SUCCESS, plan);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
		return new NonDataPlanExecuteResult(SUCCESS, plan);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
		InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
		// TODO 没找到 DeletePredicateRequest 的例子
		return new NonDataPlanExecuteResult(FAILURE, plan);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
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
	protected NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
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
	protected AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan) {
		return null;
	}

	@Override
	protected StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
		return null;
	}

	@Override
	protected StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
		return null;
	}

	@Override
	protected SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
		return null;
	}

	@Override
	protected SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
		return null;
	}

	@Override
	protected SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
		return null;
	}

	@Override
	protected SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
		return null;
	}

	@Override
	public StorageEngineChangeHook getStorageEngineChangeHook() {
		return null;
	}
}
