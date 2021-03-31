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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;

public class InfluxDBPlanExecutor extends AbstractPlanExecutor {

	private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

	private static final String QUERY_DATA = "from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\" and r._field == \"%s\" %s)";

	private Map<Long, InfluxDBClient> storageEngineIdToClient;

	private void createConnection(StorageEngineMeta storageEngineMeta) {
		if (storageEngineMeta.getDbType() != StorageEngine.InfluxDB) {
			logger.warn("unexpected database: " + storageEngineMeta.getDbType());
			return;
		}
		Map<String, String> extraParams = storageEngineMeta.getExtraParams();
		String url = extraParams.getOrDefault("url", "http://localhost:8086/");
		// TODO 处理 token
		InfluxDBClient client = InfluxDBClientFactory.create(url, ConfigDescriptor.getInstance().getConfig().getToken().toCharArray());
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
		// TODO 处理 organization 和 bucket 名称
		Organization organization = client.getOrganizationsApi()
				.findOrganizations().stream()
				.filter(o -> o.getName().equals("my-org"))
				.findFirst()
				.orElseThrow(IllegalStateException::new);
		Bucket bucket = client.getBucketsApi()
				.findBucketsByOrgName("my-org").stream()
				.filter(b -> b.getName().equals("my-bucket"))
				.findFirst()
				.orElseThrow(IllegalStateException::new);

		List<Point> points = new ArrayList<>();
		for (int i = 0; i < plan.getPathsNum(); i++) {
			String[] elements = plan.getPath(i).split("\\.");
			String measurement = elements[0];
			String field = elements[elements.length - 1];
			Map<String, String> tags = new HashMap<>();
			for (int j = 1; j < elements.length - 1; j++) {
				String[] entry = elements[j].split("-");
				tags.put(entry[0], entry[1]);
			}
			Object[] values = (Object[]) (plan.getValuesList()[i]);
			for (int j = 0; j < plan.getTimestamps().length; j++) {
				// TODO 增加处理精度
				switch (plan.getDataType(i)) {
					case BOOLEAN:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (boolean) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					case INTEGER:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (int) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					case LONG:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (long) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					case FLOAT:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (float) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					case DOUBLE:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (double) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					case STRING:
						points.add(Point.measurement(measurement).addTags(tags).addField(field, (String) values[j]).time(plan.getTimestamp(j), WritePrecision.MS));
						break;
					default:
						throw new UnsupportedDataTypeException(plan.getDataType(i).toString());
				}
			}
		}
		client.getWriteApi().writePoints(bucket.getId(), organization.getId(), points);
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
				.filter(o -> o.getName().equals("my-org"))
				.findFirst()
				.orElseThrow(IllegalStateException::new);

		List<FluxTable> tableList = new ArrayList<>();
		for (String path : plan.getPaths()) {
			String[] elements = path.split("\\.");
			String measurement = elements[0];
			String field = elements[elements.length - 1];
			StringBuilder tag = new StringBuilder();
			for (int i = 1; i < elements.length - 1; i++) {
				String[] entry = elements[i].split("-");
				tag.append("and ");
				tag.append(entry[0]);
				tag.append(" == \"");
				tag.append(entry[1]);
				tag.append("\"");
			}
			// TODO 处理时间戳
			List<FluxTable> tables = client.getQueryApi().query(String.format(QUERY_DATA, "my-bucket", plan.getStartTime(), plan.getEndTime(), measurement, field, tag), organization.getId());
			tableList.addAll(tables);
		}

		return new QueryDataPlanExecuteResult(SUCCESS, plan, Collections.singletonList(new InfluxDBQueryExecuteDataSet(tableList)));
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
		// TODO 用 $ 分隔
		String organizationName = plan.getDatabaseName().substring(0, plan.getDatabaseName().indexOf("$"));
		String bucketName = plan.getDatabaseName().substring(plan.getDatabaseName().indexOf("$") + 1);
		// TODO 只能通过 orgId 查找 organization
		Organization organization = client.getOrganizationsApi().createOrganization(organizationName);
		client.getBucketsApi().createBucket(bucketName, organization);
		return new NonDataPlanExecuteResult(SUCCESS, plan);
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
		InfluxDBClient client = storageEngineIdToClient.get(plan.getStorageEngineId());
		// TODO 用 $ 分隔
		String organizationName = plan.getDatabaseName().substring(0, plan.getDatabaseName().indexOf("$"));
		String bucketName = plan.getDatabaseName().substring(plan.getDatabaseName().indexOf("$") + 1);
		List<Bucket> buckets = client.getBucketsApi().findBucketsByOrgName(organizationName);
		for (Bucket bucket : buckets) {
			if (bucketName.equals(bucket.getName())) {
				client.getBucketsApi().deleteBucket(bucket);
				return new NonDataPlanExecuteResult(SUCCESS, plan);
			}
		}
		return new NonDataPlanExecuteResult(FAILURE, plan);
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
