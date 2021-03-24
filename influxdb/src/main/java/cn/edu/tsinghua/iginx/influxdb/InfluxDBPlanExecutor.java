package cn.edu.tsinghua.iginx.influxdb;

import cn.edu.tsinghua.iginx.core.db.StorageEngine;
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
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;

public class InfluxDBPlanExecutor extends AbstractPlanExecutor {

	private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

	private Map<Long, InfluxDBClient> storageEngineIdToClient;

	private void createConnection(StorageEngineMeta storageEngineMeta) {
		if (storageEngineMeta.getDbType() != StorageEngine.InfluxDB) {
			logger.warn("unexpected database: " + storageEngineMeta.getDbType());
			return;
		}
		Map<String, String> extraParams = storageEngineMeta.getExtraParams();
		String url = extraParams.getOrDefault("url", "http://localhost:8086/");
		InfluxDBClient client;
		if (extraParams.containsKey("username") && extraParams.containsKey("password")) {
			client = InfluxDBClientFactory.create(url, extraParams.get("username"), extraParams.get("password").toCharArray());
		} else {
			client = InfluxDBClientFactory.create(url);
		}
		storageEngineIdToClient.put(storageEngineMeta.getId(), client);
	}

	public InfluxDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
		storageEngineIdToClient = new ConcurrentHashMap<>();
		for (StorageEngineMeta storageEngineMeta : storageEngineMetaList) {
			createConnection(storageEngineMeta);
		}
	}

	@Override
	protected NonDataPlanExecuteResult syncExecuteInsertRecordsPlan(InsertRecordsPlan plan) {

		return null;
	}

	@Override
	protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
		return null;
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
		Bucket bucket;
		if ((bucket = client.getBucketsApi().findBucketByName(plan.getDatabaseName())) != null) {
			client.getBucketsApi().deleteBucket(bucket);
			return new NonDataPlanExecuteResult(SUCCESS, plan);
		} else {
			return new NonDataPlanExecuteResult(FAILURE, plan);
		}
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
