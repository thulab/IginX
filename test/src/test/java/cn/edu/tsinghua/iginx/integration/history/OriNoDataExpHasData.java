package cn.edu.tsinghua.iginx.integration.history;

public class OriNoDataExpHasData extends IoTDBHistoryDataGenerator {

    public OriNoDataExpHasData() throws Exception {
        super();
        this.oriHasData = true;
        this.expansionHasData = true;
        this.init();
    }
}