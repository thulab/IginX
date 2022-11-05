package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.TagIT;
import org.junit.Test;

public interface IoTDBBaseScaleOutIT {

    @Test
    void DBConf() throws Exception ;

    @Test
    public void OriHasDataExpHasData_IT() throws Exception;

    @Test
    public void OriHasDataExpNoData_IT() throws Exception;

    @Test
    public void OriNoDataExpHasData_IT() throws Exception;

    @Test
    public void OriNoDataExpNoData_IT() throws Exception;
}
