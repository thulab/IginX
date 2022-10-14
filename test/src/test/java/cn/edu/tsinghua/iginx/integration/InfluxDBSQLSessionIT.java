package cn.edu.tsinghua.iginx.integration;

public class InfluxDBSQLSessionIT extends SQLSessionIT {
    public InfluxDBSQLSessionIT() {
        super();
        this.isAbleToDelete = false;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = false;
    }
}
