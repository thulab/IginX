package cn.edu.tsinghua.iginx.integration;

public class IoTDB11SQLSessionCapExpIT extends SQLSessionCapExpIT {
    public IoTDB11SQLSessionCapExpIT() {
        super();
        this.isAbleToDelete = true;
        this.isAbleToShowTimeSeries = true;
        this.storageEngineType = "iotdb11";
    }
}
