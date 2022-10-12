package cn.edu.tsinghua.iginx.integration;

public class IoTDB12SQLSessionCapExpIT extends SQLSessionCapExpIT {
    public IoTDB12SQLSessionCapExpIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
        this.storageEngineType = "iotdb12";
    }
}
