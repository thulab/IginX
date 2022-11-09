package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.TagIT;
import org.junit.Test;

public interface IoTDBBaseScaleOutIT {

    void DBConf() throws Exception ;

    @Test
    public void OriHasDataExpHasDataIT() throws Exception;

    @Test
    public void OriHasDataExpNoDataIT() throws Exception;

    @Test
    public void OriNoDataExpHasDataIT() throws Exception;

    @Test
    public void OriNoDataExpNoDataIT() throws Exception;
}
