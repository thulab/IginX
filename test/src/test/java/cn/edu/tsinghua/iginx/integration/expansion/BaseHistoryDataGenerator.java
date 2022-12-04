package cn.edu.tsinghua.iginx.integration.expansion;

import org.junit.Test;

public interface BaseHistoryDataGenerator {
    @Test
    public void writeHistoryDataToB() throws Exception;

    @Test
    public void writeHistoryDataToA() throws Exception;
}
