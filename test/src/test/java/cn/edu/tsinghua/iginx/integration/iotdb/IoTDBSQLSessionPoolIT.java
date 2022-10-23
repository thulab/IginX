package cn.edu.tsinghua.iginx.integration.iotdb;

import cn.edu.tsinghua.iginx.integration.SQLSessionPoolIT;

public class IoTDBSQLSessionPoolIT extends SQLSessionPoolIT {

    public IoTDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
