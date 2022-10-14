package cn.edu.tsinghua.iginx.integration;

public class ParquetSQLSessionPoolIT extends SQLSessionPoolIT {

    public ParquetSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
