package cn.edu.tsinghua.iginx.metadatav2;


import java.util.List;
import java.util.Map;

public interface IMetaManager {

    /**
     * 新增数据库节点
     */
    boolean addStorageEngine(StorageEngineMeta storageEngineMeta);

    /**
     * 获取所有的数据库实例的原信息（不包括每个数据库的分片列表）
     */
    List<StorageEngineMeta> getStorageEngineList();

    /**
     * 获取某个时序数据库的所有分片的元信息
     */
    List<FragmentReplicaMeta> getFragmentListByDatabase(long databaseId);

    /**
     * 获取所有活跃的 iginx 节点的元信息
     */
    List<IginxMeta> getIginxList();

    /**
     * 获取当前 iginx 节点的 ID
     */
    long getId();

    /**
     获取某个时间序列区间的所有分片。
     */
    Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     获取某个时间区间的所有最新的分片（这些分片一定也都是未终结的分片）。
     */
    Map<String, FragmentMeta> getLatestFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     获取某个时间序列区间在某个时间区间的所有分片。
     */
    Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval,
                                                                                       TimeInterval timeInterval);

    /**
     获取某个时间序列的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentByTimeSeriesName(String tsName);

    /**
     获取某个时间序列的最新分片
     */
    FragmentMeta getLatestFragmentByTimeSeriesName(String tsName);


    /**
     获取某个时间序列在某个时间区间的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval);

    /**
     创建时间分片
     */
    boolean createFragment(FragmentMeta fragment);

}
