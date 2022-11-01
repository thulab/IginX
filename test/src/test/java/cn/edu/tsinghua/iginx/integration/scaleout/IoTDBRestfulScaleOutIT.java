package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import org.junit.Test;

public class IoTDBRestfulScaleOutIT extends RestIT implements IoTDBBaseScaleOutIT{
    public IoTDBRestfulScaleOutIT() {
        super();
    }

    public void iotdb11_IT() throws Exception {
        this.ifClearData = false;
        this.storageEngineType = "iotdb11";
    }

    @Test
    public void iotdb11_OriHasDataExpHasData_IT() throws Exception {
        iotdb11_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriHasDataExpNoData_IT() throws Exception {
        iotdb11_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriNoDataExpHasData_IT() throws Exception {
        iotdb11_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriNoDataExpNoData_IT() throws Exception {
        iotdb11_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_IT() throws Exception {
        this.ifClearData = false;
        this.storageEngineType = "iotdb12";
    }

    @Test
    public void iotdb12_OriHasDataExpHasData_IT() throws Exception {
        iotdb12_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriHasDataExpNoData_IT() throws Exception {
        iotdb12_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriNoDataExpHasData_IT() throws Exception {
        iotdb12_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriNoDataExpNoData_IT() throws Exception {
        iotdb12_IT();
        RestIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }
}
