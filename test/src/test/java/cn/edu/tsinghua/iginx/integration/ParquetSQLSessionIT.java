package cn.edu.tsinghua.iginx.integration;

public class ParquetSQLSessionIT extends SQLSessionIT {
    public ParquetSQLSessionIT() {
        super();
        this.isAbleToDelete = true;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = true;
    }
}
