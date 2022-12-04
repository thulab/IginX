package cn.edu.tsinghua.iginx.integration.scaleout;

import cn.edu.tsinghua.iginx.integration.TagIT;
import org.junit.Test;

public interface IoTDBBaseScaleOutIT {

    void DBConf() throws Exception ;

    @Test
    public void oriHasDataExpHasDataIT() throws Exception;

    @Test
    public void oriHasDataExpNoDataIT() throws Exception;

    @Test
    public void oriNoDataExpHasDataIT() throws Exception;

    @Test
    public void oriNoDataExpNoDataIT() throws Exception;
}
