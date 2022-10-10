package cn.edu.tsinghua.iginx.integration.history;

public class OriHasDataExpHasData  extends IoTDBHistoryDataGenerator {

    public OriHasDataExpHasData() throws Exception {
        super();
        this.oriHasData = true;
        this.expansionHasData = true;
        this.init();
    }
}