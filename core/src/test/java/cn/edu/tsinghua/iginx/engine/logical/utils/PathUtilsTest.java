package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PathUtilsTest {

    @Test
    public void test() {
        TimeSeriesInterval interval1 = new TimeSeriesInterval("*", "*");
        TimeSeriesInterval expected1 = new TimeSeriesInterval(null, null);
        assertEquals(expected1, PathUtils.trimTimeSeriesInterval(interval1));

        TimeSeriesInterval interval2 = new TimeSeriesInterval("a.*", "*.c");
        TimeSeriesInterval expected2 = new TimeSeriesInterval("a.!", null);
        assertEquals(expected2, PathUtils.trimTimeSeriesInterval(interval2));

        TimeSeriesInterval interval3 = new TimeSeriesInterval("*.d", "b.*");
        TimeSeriesInterval expected3 = new TimeSeriesInterval(null, "b.~");
        assertEquals(expected3, PathUtils.trimTimeSeriesInterval(interval3));

        TimeSeriesInterval interval4 = new TimeSeriesInterval("a.*.c", "b.*.c");
        TimeSeriesInterval expected4 = new TimeSeriesInterval("a.!", "b.~");
        assertEquals(expected4, PathUtils.trimTimeSeriesInterval(interval4));

        TimeSeriesInterval interval5 = new TimeSeriesInterval("a.*.*.c", "b.*.*.*.c");
        TimeSeriesInterval expected5 = new TimeSeriesInterval("a.!", "b.~");
        assertEquals(expected5, PathUtils.trimTimeSeriesInterval(interval5));
    }
}
