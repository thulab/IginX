package cn.edu.tsinghua.iginx.transform.api;

import cn.edu.tsinghua.iginx.transform.data.BatchData;

public interface Reader {

    boolean hasNextBatch();

    BatchData loadNextBatch();

    void close();
}
