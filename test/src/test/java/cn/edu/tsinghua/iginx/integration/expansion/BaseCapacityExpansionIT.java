package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseCapacityExpansionIT {
    @Test
    public void OriHasDataExpHasData() throws Exception;

    @Test
    public void OriHasDataExpNoData() throws Exception;

    @Test
    public void OriNoDataExpHasData() throws Exception;

    @Test
    public void OriNoDataExpNoData() throws Exception;
}
