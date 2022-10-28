package cn.edu.tsinghua.iginx.integration.parquet;

import cn.edu.tsinghua.iginx.integration.SQLSessionIT;

public class ParquetSQLSessionIT extends SQLSessionIT {
    public ParquetSQLSessionIT() {
        super();
        this.isAbleToDelete = true;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = true;
    }
}
