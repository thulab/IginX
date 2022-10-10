package cn.edu.tsinghua.iginx.integration.history;

public class OriHasDataExpNoData extends IoTDBHistoryDataGenerator {

    public OriHasDataExpNoData() throws Exception {
        super();
        this.oriHasData = true;
        this.expansionHasData = true;
        this.init();
    }
}
