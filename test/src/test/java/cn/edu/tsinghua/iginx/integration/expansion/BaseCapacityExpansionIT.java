package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseCapacityExpansionIT {
    @Test
    public void oriHasDataExpHasData() throws Exception;

    @Test
    public void oriHasDataExpNoData() throws Exception;

    @Test
    public void oriNoDataExpHasData() throws Exception;

    @Test
    public void oriNoDataExpNoData() throws Exception;
}
