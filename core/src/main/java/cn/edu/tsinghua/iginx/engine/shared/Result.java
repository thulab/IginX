package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
public class Result {

    private static final Logger logger = LoggerFactory.getLogger(Result.class);

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

    private List<RegisterTaskInfo> registerTaskInfos;

    private long queryId;
    private JobState jobState;
    private RowStream resultStream;

    private long jobId;

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

    public QueryDataResp getRestQueryDataResp(boolean ifAggregate) {
        QueryDataResp resp = new QueryDataResp(status);
        resp.setPaths(paths);
        resp.setDataTypeList(dataTypes);
        if (timestamps == null || timestamps.length == 0) {
            if(ifAggregate){
                //这里仅仅是为了添加一个无意义值
                timestamps = new Long[1];
                timestamps[0] = -1L;
            }else{
                resp.setQueryDataSet(new QueryDataSet(ByteBuffer.allocate(0), new ArrayList<>(), new ArrayList<>()));
                return resp;
            }
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
        resp.setRegisterTaskInfos(registerTaskInfos);
        resp.setJobId(jobId);
        resp.setJobState(jobState);
        return resp;
    }

    public ExecuteStatementResp getExecuteStatementResp(int fetchSize) {
        ExecuteStatementResp resp = new ExecuteStatementResp(status, sqlType);
        if (status != RpcUtils.SUCCESS) {
            return resp;
        }
        resp.setQueryId(queryId);
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> types = new ArrayList<>();

            Header header = resultStream.getHeader();

            if (header.hasTimestamp()) {
                paths.add(Field.TIME.getFullName());
                types.add(Field.TIME.getType());
            }

            resultStream.getHeader().getFields().forEach(field -> {
                paths.add(field.getFullName());
                types.add(field.getType());
            });

            List<ByteBuffer> valuesList = new ArrayList<>();
            List<ByteBuffer> bitmapList = new ArrayList<>();

            int cnt = 0;
            boolean hasTimestamp = resultStream.getHeader().hasTimestamp();
            while (resultStream.hasNext() && cnt < fetchSize) {
                Row row = resultStream.next();

                Object[] rawValues = row.getValues();
                Object[] rowValues = rawValues;
                if (hasTimestamp) {
                    rowValues = new Object[rawValues.length + 1];
                    rowValues[0] = row.getTimestamp();
                    System.arraycopy(rawValues, 0, rowValues, 1, rawValues.length);
                }
                valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));

                Bitmap bitmap = new Bitmap(rowValues.length);
                for (int i = 0; i < rowValues.length; i++) {
                    if (rowValues[i] != null) {
                        bitmap.mark(i);
                    }
                }
                bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
                cnt++;
            }
            resp.setColumns(paths);
            resp.setDataTypeList(types);
            resp.setQueryDataSet(new QueryDataSetV2(valuesList, bitmapList));
        } catch (PhysicalException e) {
            logger.error("unexpected error when load row stream: ", e);
            resp.setStatus(RpcUtils.FAILURE);
        }
        return resp;
    }

    public FetchResultsResp fetch(int fetchSize) {
        FetchResultsResp resp = new FetchResultsResp(status, false);

        if (status != RpcUtils.SUCCESS) {
            return resp;
        }
        try {
            List<DataType> types = new ArrayList<>();

            Header header = resultStream.getHeader();

            if (header.hasTimestamp()) {
                types.add(Field.TIME.getType());
            }

            resultStream.getHeader().getFields().forEach(field -> types.add(field.getType()));

            List<ByteBuffer> valuesList = new ArrayList<>();
            List<ByteBuffer> bitmapList = new ArrayList<>();

            int cnt = 0;
            boolean hasTimestamp = resultStream.getHeader().hasTimestamp();
            while (resultStream.hasNext() && cnt < fetchSize) {
                Row row = resultStream.next();

                Object[] rawValues = row.getValues();
                Object[] rowValues = rawValues;
                if (hasTimestamp) {
                    rowValues = new Object[rawValues.length + 1];
                    rowValues[0] = row.getTimestamp();
                    System.arraycopy(rawValues, 0, rowValues, 1, rawValues.length);
                }
                valuesList.add(ByteUtils.getRowByteBuffer(rowValues, types));

                Bitmap bitmap = new Bitmap(rowValues.length);
                for (int i = 0; i < rowValues.length; i++) {
                    if (rowValues[i] != null) {
                        bitmap.mark(i);
                    }
                }
                bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
                cnt++;
            }
            resp.setHasMoreResults(resultStream.hasNext());
            resp.setQueryDataSet(new QueryDataSetV2(valuesList, bitmapList));
        } catch (PhysicalException e) {
            logger.error("unexpected error when load row stream: ", e);
            resp.setStatus(RpcUtils.FAILURE);
        }
        return resp;
    }



}
