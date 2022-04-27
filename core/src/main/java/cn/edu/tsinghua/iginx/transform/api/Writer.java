package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.transform.data.BatchData;
import cn.edu.tsinghua.iginx.transform.exception.WriteBatchException;

public interface Writer {

    void writeBatch(BatchData batchData) throws WriteBatchException;

}
