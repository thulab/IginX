package cn.edu.tsinghua.iginx.integration;

public class IoTDBSQLSessionPoolIT extends SQLSessionPoolIT {

    public IoTDBSQLSessionPoolIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
    }
}
