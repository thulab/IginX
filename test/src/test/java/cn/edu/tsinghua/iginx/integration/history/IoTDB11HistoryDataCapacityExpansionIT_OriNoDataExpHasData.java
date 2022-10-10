package cn.edu.tsinghua.iginx.integration.history;

public class IoTDB11HistoryDataCapacityExpansionIT_OriNoDataExpHasData  extends IoTDBHistoryDataCapacityExpansionIT {

    public IoTDB11HistoryDataCapacityExpansionIT_OriNoDataExpHasData() {
        super("iotdb11");
        this.oriNoDataExpHasData = true;
    }
}