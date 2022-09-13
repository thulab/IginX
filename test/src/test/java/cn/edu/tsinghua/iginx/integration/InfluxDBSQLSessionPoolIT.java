package cn.edu.tsinghua.iginx.integration;

public class InfluxDBSQLSessionPoolIT extends SQLSessionPoolIT {
    public InfluxDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = false;
        this.isAbleToShowTimeSeries = false;
    }
}
