package cn.edu.tsinghua.iginx.integration.iotdb;

import cn.edu.tsinghua.iginx.integration.SQLSessionIT;

public class IoTDBSQLSessionIT extends SQLSessionIT {
    public IoTDBSQLSessionIT() {
        super();
        this.isAbleToDelete = true;
        this.isSupportSpecialPath = true;
        this.isAbleToShowTimeSeries = true;
    }
}
