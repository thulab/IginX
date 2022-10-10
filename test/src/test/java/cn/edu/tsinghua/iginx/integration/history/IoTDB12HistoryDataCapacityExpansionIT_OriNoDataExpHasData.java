package cn.edu.tsinghua.iginx.integration.history;

public class IoTDB12HistoryDataCapacityExpansionIT_OriNoDataExpHasData extends IoTDBHistoryDataCapacityExpansionIT {

    public IoTDB12HistoryDataCapacityExpansionIT_OriNoDataExpHasData() {
        super("iotdb12");
        this.oriNoDataExpHasData = true;
    }
}