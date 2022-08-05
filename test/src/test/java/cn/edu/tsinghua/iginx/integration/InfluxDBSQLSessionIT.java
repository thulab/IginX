package cn.edu.tsinghua.iginx.integration;

public class InfluxDBSQLSessionIT extends SQLSessionIT {
    public InfluxDBSQLSessionIT() {
        super();
        this.isAbleToDelete = false;
        this.isAbleToShowTimeSeries = false;
    }
}
