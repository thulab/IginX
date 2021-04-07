
package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class SortedListAbstractMetaManagerTest {
    public static SortedListAbstractMetaManager metaManager;

    @BeforeClass
    public static void init() {
        try {
            metaManager = new SortedListAbstractMetaManager();
            System.out.println("1 finish");
            StorageEngineMeta sem = new StorageEngineMeta(
                    1, "127.0.0.1", 6666, null, StorageEngine.IoTDB);
            metaManager.addStorageEngine(sem);
            TimeInterval t = new TimeInterval(0, 100);
            TimeInterval t2 = new TimeInterval(100, 200);
            TimeInterval t3 = new TimeInterval(100, Long.MAX_VALUE);
            TimeInterval t4 = new TimeInterval(200, Long.MAX_VALUE);
            TimeSeriesInterval tsi = new TimeSeriesInterval("root.a.a.a", "root.a.a.b");
            TimeSeriesInterval tsi2 = new TimeSeriesInterval("root.b.a", "root.b.b");
            FragmentReplicaMeta frm = new FragmentReplicaMeta(t, tsi, 0, 1);
            FragmentReplicaMeta frm2 = new FragmentReplicaMeta(t2, tsi, 1, 1);
            FragmentReplicaMeta frm3 = new FragmentReplicaMeta(t, tsi2, 2, 1);
            FragmentReplicaMeta frm4 = new FragmentReplicaMeta(t3, tsi2, 3, 1);
            FragmentReplicaMeta frm5 = new FragmentReplicaMeta(t4, tsi, 4, 1);
            Map<Integer, FragmentReplicaMeta> m = new ConcurrentHashMap<>();
            Map<Integer, FragmentReplicaMeta> m2 = new ConcurrentHashMap<>();
            Map<Integer, FragmentReplicaMeta> m3 = new ConcurrentHashMap<>();
            Map<Integer, FragmentReplicaMeta> m4 = new ConcurrentHashMap<>();
            Map<Integer, FragmentReplicaMeta> m5 = new ConcurrentHashMap<>();
            m.put(0, frm);
            m2.put(1, frm2);
            m3.put(2, frm3);
            m4.put(3, frm4);
            m5.put(4, frm5);
            FragmentMeta fm = new FragmentMeta("root.a.a.a", "root.a.a.b", 0, 100, m);
            FragmentMeta fm2 = new FragmentMeta("root.a.a.a", "root.a.a.b", 100, 200, m2);
            FragmentMeta fm3 = new FragmentMeta("root.b.a", "root.b.b", 0, 100, m3);
            FragmentMeta fm4 = new FragmentMeta("root.b.a", "root.b.b", 100, Long.MAX_VALUE, m4);
            FragmentMeta fm5 = new FragmentMeta("root.a.a.a", "root.a.a.b", 200, Long.MAX_VALUE, m5);
            Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap = new ConcurrentHashMap<>();
            List<FragmentMeta> fragmentList = new LinkedList<>();
            fragmentList.add(fm);
            fragmentList.add(fm2);
            fragmentList.add(fm5);
            List<FragmentMeta> fragmentList2 = new LinkedList<>();
            fragmentList2.add(fm3);
            fragmentList2.add(fm4);
            fragmentListMap.put(tsi, fragmentList);
            fragmentListMap.put(tsi2, fragmentList2);
            metaManager.initFragment(fragmentListMap);
        } catch (Exception e) {
            System.out.println("Error occurs:" + e);
        }
    }

    @Test
    public void testGetFragmentListByTimeSeriesInterval() {
        TimeSeriesInterval tsi = new TimeSeriesInterval("root.a.a.a", "root.a.a.b");
        Map<TimeSeriesInterval, List<FragmentMeta>> m = metaManager.getFragmentMapByTimeSeriesInterval(tsi);
        List<FragmentMeta> l = m.get(tsi);
        assertEquals(l.size(),3);
        assertEquals(l.get(1).getTimeInterval().getStartTime(), 100);
        assertEquals(l.get(0).getTimeInterval().getEndTime(), 100);
    }

    @Test
    public void testGetLatestFragmentMap() {
        TimeSeriesInterval tsi = new TimeSeriesInterval("root.a.a.a", "root.a.a.b");
        TimeSeriesInterval tsi2 = new TimeSeriesInterval("root.b.a", "root.b.b");
        Map<TimeSeriesInterval, FragmentMeta> m = metaManager.getLatestFragmentMap();
        assertEquals(m.get(tsi).getTimeInterval().getStartTime(), 200);
        assertEquals(m.get(tsi2).getTimeInterval().getStartTime(), 100);
    }

    @Test
    public void testGetLatestFragmentListByTimeSeriesInterval() {
        TimeSeriesInterval tsi = new TimeSeriesInterval("root.a.a.a", "root.a.a.b");
        Map<TimeSeriesInterval, FragmentMeta> m = metaManager.getLatestFragmentMapByTimeSeriesInterval(tsi);
        FragmentMeta f = m.get(tsi);
        assertEquals(f.getReplicaMetasNum(), 1);
    }

    @Test
    public void testGetFragmentListByTimeSeriesIntervalAndTimeInterval() {
        TimeSeriesInterval tsi = new TimeSeriesInterval("root.a.a.a", "root.a.a.b");
        TimeInterval ti = new TimeInterval(110, 300);
        Map<TimeSeriesInterval, List<FragmentMeta>> m = metaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(tsi, ti);
        assertEquals(m.get(tsi).size(), 2);
        assertEquals(m.get(tsi).get(0).getTimeInterval().getStartTime(), 100);
        assertEquals(m.get(tsi).get(1).getTimeInterval().getStartTime(), 200);
    }

    @Test
    public void testGetFragmentListByTimeSeriesName() {
        List<FragmentMeta> lfm = metaManager.getFragmentListByTimeSeriesName("root.a.a.a");
        assertEquals(lfm.size(), 3);
        assertEquals(lfm.get(0).getTimeInterval().getEndTime(), 100);
        assertEquals(lfm.get(1).getTimeInterval().getEndTime(), 200);
    }

    @Test
    public void testGetLatestFragmentByTimeSeriesName() {
        FragmentMeta fm = metaManager.getLatestFragmentByTimeSeriesName("root.b.a.b");
        assertEquals(fm.getTimeInterval().getStartTime(), 100);
    }

    @Test
    public void testGetFragmentListByTimeSeriesNameAndTimeInterval() {
        TimeInterval ti = new TimeInterval(110, 300);
        List<FragmentMeta> lfm = metaManager.getFragmentListByTimeSeriesNameAndTimeInterval("root.a.a.a", ti);
        assertEquals(lfm.size(), 2);
        assertEquals(lfm.get(0).getTimeInterval().getStartTime(), 100);
        assertEquals(lfm.get(1).getTimeInterval().getStartTime(), 200);
    }

}
