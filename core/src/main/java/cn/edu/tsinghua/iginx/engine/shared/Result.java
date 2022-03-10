package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import lombok.Data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
public class Result {

    private Status status;
    private List<String> paths;
    private List<DataType> dataTypes;
    private Long[] timestamps;
    private List<ByteBuffer> valuesList;
    private List<ByteBuffer> bitmapList;

    private SqlType sqlType;
    private Long pointsNum;
    private Integer replicaNum;

    private List<IginxInfo> iginxInfos;
    private List<StorageEngineInfo> storageEngineInfos;
    private List<MetaStorageInfo> metaStorageInfos;
    private LocalMetaStorageInfo localMetaStorageInfo;

    public Result(Status status) {
        this.status = status;
        this.pointsNum = 0L;
        this.replicaNum = 0;
    }

    public QueryDataResp getQueryDataResp() {
        QueryDataResp resp = new QueryDataResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        if (timestamps == null || timestamps.length == 0) {
            resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), new ArrayList<>(), new ArrayList<>()));
            return resp;
        }
        ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
        resp.setQueryDataSet(new QueryDataSet(timeBuffer, valuesList, bitmapList));
        return resp;
    }

    public AggregateQueryResp getAggregateQueryResp() {
        AggregateQueryResp resp = new AggregateQueryResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        if (valuesList == null || valuesList.size() == 0) {
            resp.setValuesList(ByteBuffer.allocate(0));
            return resp;
        }
        resp.setValuesList(valuesList.get(0));
        return resp;
    }

    public DownsampleQueryResp getDownSampleQueryResp() {
        DownsampleQueryResp resp = new DownsampleQueryResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        if (timestamps == null || timestamps.length == 0) {
            resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), new ArrayList<>(), new ArrayList<>()));
            return resp;
        }
        ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
        resp.setQueryDataSet(new QueryDataSet(timeBuffer, valuesList, bitmapList));
        return resp;
    }

    public LastQueryResp getLastQueryResp() {
        LastQueryResp resp = new LastQueryResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        if (timestamps == null || timestamps.length == 0) {
            resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), new ArrayList<>(), new ArrayList<>()));
            return resp;
        }
        ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
        resp.setQueryDataSet(new QueryDataSet(timeBuffer, valuesList, bitmapList));
        return resp;
    }

    public ShowColumnsResp getShowColumnsResp() {
        ShowColumnsResp resp = new ShowColumnsResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        return resp;
    }

    public ExecuteSqlResp getExecuteSqlResp() {
        ExecuteSqlResp resp = new ExecuteSqlResp(status, sqlType);
        if (status != RpcUtils.SUCCESS) {
            resp.setParseErrorMsg(status.getMessage());
            return resp;
        }

        resp.setReplicaNum(replicaNum);
        resp.setPointsNum(pointsNum);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);

        if (valuesList != null) {
            if (timestamps != null) {
                ByteBuffer timeBuffer = ByteUtils.getByteBufferFromLongArray(timestamps);
                resp.setTimestamps(timeBuffer);
                resp.setQueryDataSet(new QueryDataSet(timeBuffer, valuesList, bitmapList));
            } else {
                resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), valuesList, bitmapList));
            }
        }

        resp.setIginxInfos(iginxInfos);
        resp.setStorageEngineInfos(storageEngineInfos);
        resp.setMetaStorageInfos(metaStorageInfos);
        resp.setLocalMetaStorageInfo(localMetaStorageInfo);
        return resp;
    }

}
