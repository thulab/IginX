package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.BaseSessionIT;
import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import org.junit.Test;

import java.util.LinkedHashMap;

public class IoTDBSessionScaleOutIT extends BaseSessionIT implements IoTDBBaseScaleOutIT{
    public IoTDBSessionScaleOutIT() {
        super();
    }

    @Test
    public void iotdb11_IT() throws Exception {
        this.defaultPort2 = 6668;
        this.isAbleToDelete = false;
        this.ifNeedCapExp = false;
        this.storageEngineType = "iotdb11";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");

        this.ifClearData = false;
    }

    @Test
    public void iotdb11_OriHasDataExpHasData_IT() throws Exception {
        iotdb11_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriHasDataExpNoData_IT() throws Exception {
        iotdb11_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriNoDataExpHasData_IT() throws Exception {
        iotdb11_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb11_OriNoDataExpNoData_IT() throws Exception {
        iotdb11_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_IT() throws Exception {
        this.defaultPort2 = 6668;
        this.isAbleToDelete = false;
        this.ifNeedCapExp = false;
        this.storageEngineType = "iotdb12";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");

        this.ifClearData = false;
    }

    @Test
    public void iotdb12_OriHasDataExpHasData_IT() throws Exception {
        iotdb12_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriHasDataExpNoData_IT() throws Exception {
        iotdb12_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriNoDataExpHasData_IT() throws Exception {
        iotdb12_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void iotdb12_OriNoDataExpNoData_IT() throws Exception {
        iotdb12_IT();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

}
