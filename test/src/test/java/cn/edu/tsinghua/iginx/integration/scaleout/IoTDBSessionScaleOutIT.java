package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.BaseSessionIT;
import cn.edu.tsinghua.iginx.integration.rest.RestIT;
import cn.edu.tsinghua.iginx.utils.FileReader;
import org.junit.Test;

import java.util.LinkedHashMap;

public class IoTDBSessionScaleOutIT extends BaseSessionIT implements IoTDBBaseScaleOutIT{
    public IoTDBSessionScaleOutIT() {
        super();
    }

    public void DBConf() throws Exception {
        this.defaultPort2 = 6668;
        this.isAbleToDelete = false;
        this.ifNeedCapExp = false;
        this.storageEngineType = "iotdb11";
        this.extraParams = new LinkedHashMap<>();
        this.extraParams.put("username", "root");
        this.extraParams.put("password", "root");
        this.extraParams.put("sessionPoolSize", "100");
        this.storageEngineType = FileReader.convertToString("./src/test/java/cn/edu/tsinghua/iginx/integration/DBConf.txt");

        this.ifClearData = false;
    }

    @Test
    public void OriHasDataExpHasData_IT() throws Exception {
        DBConf();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void OriHasDataExpNoData_IT() throws Exception {
        DBConf();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void OriNoDataExpHasData_IT() throws Exception {
        DBConf();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");
        capacityExpansion();
    }

    @Test
    public void OriNoDataExpNoData_IT() throws Exception {
        DBConf();
        BaseSessionIT.session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + storageEngineType + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");
        capacityExpansion();
    }

}
