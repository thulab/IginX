package cn.edu.tsinghua.iginx.integration.influxdb;

import cn.edu.tsinghua.iginx.integration.SQLSessionIT;

public class InfluxDBSQLSessionIT extends SQLSessionIT {
    public InfluxDBSQLSessionIT() {
        super();
        this.isAbleToDelete = false;
        this.isSupportSpecialPath = false;
        this.isAbleToShowTimeSeries = false;
    }
}
