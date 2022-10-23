package cn.edu.tsinghua.iginx.integration.parquet;

import cn.edu.tsinghua.iginx.integration.SQLSessionPoolIT;

public class ParquetSQLSessionPoolIT extends SQLSessionPoolIT {

    public ParquetSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
