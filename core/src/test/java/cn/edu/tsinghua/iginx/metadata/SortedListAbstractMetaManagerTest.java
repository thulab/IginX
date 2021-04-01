/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SortedListAbstractMetaManagerTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    @Test
    public void testSearchFragmentList() {
        // 构造分片
        List<FragmentMeta> fragmentMetas = new ArrayList<>();
        FragmentMeta fragmentMeta0 = new FragmentMeta("root.sg1.d1.s1", null, 0L, 50L, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta0);
        FragmentMeta fragmentMeta1 = new FragmentMeta(null, "root.sg1.d1.s1", 0L, 100L, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta1);
        FragmentMeta fragmentMeta2 = new FragmentMeta("root.sg1.d1.s1", null, 50L, 150L, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta2);
        FragmentMeta fragmentMeta3 = new FragmentMeta(null, "root.sg1.d1.s1", 100L, 120L, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta3);
        FragmentMeta fragmentMeta4 = new FragmentMeta(null, "root.sg1.d1.s1", 120L, 500L, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta4);
        FragmentMeta fragmentMeta5 = new FragmentMeta("root.sg1.d1.s1", null, 150L, Long.MAX_VALUE, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta5);
        FragmentMeta fragmentMeta6 = new FragmentMeta(null, "root.sg1.d1.s1", 500L, Long.MAX_VALUE, (Map<Integer, FragmentReplicaMeta>) null);
        fragmentMetas.add(fragmentMeta6);

        List<FragmentMeta> resultList;
        // 查询区间 [0, 49]
        resultList = SortedListAbstractMetaManager.searchFragmentList(fragmentMetas, new TimeInterval(0, 49));
        assertEquals(2, resultList.size());
        assertSame(fragmentMeta0, resultList.get(0));
        assertSame(fragmentMeta1, resultList.get(1));
        // 查询区间 [0, 50]
        resultList = SortedListAbstractMetaManager.searchFragmentList(fragmentMetas, new TimeInterval(0, 50));
        assertEquals(3, resultList.size());
        assertSame(fragmentMeta0, resultList.get(0));
        assertSame(fragmentMeta1, resultList.get(1));
        assertSame(fragmentMeta2, resultList.get(2));
        // 查询区间 [50, 100]
        resultList = SortedListAbstractMetaManager.searchFragmentList(fragmentMetas, new TimeInterval(50, 100));
        assertEquals(3, resultList.size());
        assertSame(fragmentMeta1, resultList.get(0));
        assertSame(fragmentMeta2, resultList.get(1));
        assertSame(fragmentMeta3, resultList.get(2));
        // 查询区间 [130, 140]
        resultList = SortedListAbstractMetaManager.searchFragmentList(fragmentMetas, new TimeInterval(130, 140));
      //  assertEquals(2, resultList.size());
        assertSame(fragmentMeta2, resultList.get(0));
        assertSame(fragmentMeta4, resultList.get(1));
        // 查询区间 [600, ~]
        resultList = SortedListAbstractMetaManager.searchFragmentList(fragmentMetas, new TimeInterval(600, Long.MAX_VALUE));
        assertEquals(2, resultList.size());
        assertSame(fragmentMeta5, resultList.get(0));
        assertSame(fragmentMeta6, resultList.get(1));
    }

    private List<Pair<TimeSeriesInterval, List<FragmentMeta>>> constructFragmentSeriesList() {
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
        resultList.add(new Pair<>(new TimeSeriesInterval(null, "root.sg1.d0.s1"), null));
        resultList.add(new Pair<>(new TimeSeriesInterval(null, "root.sg1.d1.s2"), null));
        resultList.add(new Pair<>(new TimeSeriesInterval("root.sg0.d0.s1", "root.sg1.d1.s2"), null));
        resultList.add(new Pair<>(new TimeSeriesInterval("root.sg0.d0.s1", "root.sg1.d0.s3"), null));
        resultList.add(new Pair<>(new TimeSeriesInterval("root.sg1.d0.s3", null), null));
        resultList.add(new Pair<>(new TimeSeriesInterval("root.sg1.d1.s2", null), null));
        return resultList;
    }

    @Test
    public void testSearchFragmentSeriesListByTsName() {
        // 构造数据
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList = constructFragmentSeriesList();

        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList;
        // 查询时间序列 root.sg1.d0.s1
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, "root.sg1.d0.s1");
        assertEquals(3, resultList.size());
        assertSame(fragmentSeriesList.get(1), resultList.get(0));
        assertSame(fragmentSeriesList.get(2), resultList.get(1));
        assertSame(fragmentSeriesList.get(3), resultList.get(2));
        // 查询时间序列 root.sg1.d1.s1
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, "root.sg1.d1.s1");
        assertEquals(3, resultList.size());
        assertSame(fragmentSeriesList.get(1), resultList.get(0));
        assertSame(fragmentSeriesList.get(2), resultList.get(1));
        assertSame(fragmentSeriesList.get(4), resultList.get(2));
        // 查询时间序列 root.sa1.d0.s0
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, "root.sa1.d0.s0");
        assertEquals(2, resultList.size());
        assertSame(fragmentSeriesList.get(0), resultList.get(0));
        assertSame(fragmentSeriesList.get(1), resultList.get(1));
        // 查询时间序列 root.sg1.d0.s3
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, "root.sg1.d0.s3");
        assertEquals(3, resultList.size());
        assertSame(fragmentSeriesList.get(1), resultList.get(0));
        assertSame(fragmentSeriesList.get(2), resultList.get(1));
        assertSame(fragmentSeriesList.get(4), resultList.get(2));
        // 查询时间序列 root.sg2.d0.s0
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, "root.sg2.d0.s0");
        assertEquals(2, resultList.size());
        assertSame(fragmentSeriesList.get(4), resultList.get(0));
        assertSame(fragmentSeriesList.get(5), resultList.get(1));
    }

    @Test
    public void testSearchFragmentSeriesListByTsInterval() {
        // 构造数据
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList = constructFragmentSeriesList();

        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList;
        // 查询区间 [null, null]
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, new TimeSeriesInterval(null, null));
        assertEquals(fragmentSeriesList.size(), resultList.size());
        for (int i = 0; i < fragmentSeriesList.size(); i++) {
            assertSame(fragmentSeriesList.get(i), resultList.get(i));
        }
        // 查询区间 [null, "root.sg1.d0.s1"]
        resultList = SortedListAbstractMetaManager.searchFragmentSeriesList(fragmentSeriesList, new TimeSeriesInterval(null, "root.sg1.d0.s1"));
        assertEquals(4, resultList.size());
        assertEquals(fragmentSeriesList.get(0), resultList.get(0));
        assertEquals(fragmentSeriesList.get(1), resultList.get(1));
        assertEquals(fragmentSeriesList.get(2), resultList.get(2));
        assertEquals(fragmentSeriesList.get(3), resultList.get(3));
    }

}
