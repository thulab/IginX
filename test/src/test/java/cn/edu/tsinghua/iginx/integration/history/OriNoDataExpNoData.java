package cn.edu.tsinghua.iginx.integration.history;

public class OriNoDataExpNoData extends IoTDBHistoryDataGenerator {

    public OriNoDataExpNoData() throws Exception {
        super();
        this.oriHasData = true;
        this.expansionHasData = true;
    }
}