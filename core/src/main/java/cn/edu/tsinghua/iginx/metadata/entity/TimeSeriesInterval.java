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
package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperMetaStorage;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public interface TimeSeriesInterval extends Comparable<TimeSeriesInterval> {

    public static Logger logger = LoggerFactory.getLogger(ZooKeeperMetaStorage.class);

    public static enum TYPE {
        PREFIX,
        NORMAL
    }

    public TYPE getType();

    default public boolean isNormal() {
        return getType() == TYPE.NORMAL;
    }

    default public boolean isPrefix() {
        return getType() == TYPE.PREFIX;
    }

    default public void setTimeSeries(String timeSeries) {
        if (getType() == TYPE.NORMAL) {
            logger.error("TimeSeriesInterval Normal can't not use the setTimeSeries func");
            System.exit(0);
        }
    }

    default public String getTimeSeries() {
        logger.warn("TimeSeriesInterval Normal can't not use the getTimeSeries func");
        return null;
    }

    default public String getStartTimeSeries() {
        if (getType() == TYPE.PREFIX) {
            logger.error("TimeSeriesInterval PREFIX can't not use the getStartTimeSeries func");
            System.exit(0);
        }
        return null;
    }

    default public void setStartTimeSeries(String startTimeSeries) {
        if (getType() == TYPE.PREFIX) {
            logger.error("TimeSeriesInterval PREFIX can't not use the setStartTimeSeries func");
            System.exit(0);
        }
    }

    default public String getEndTimeSeries() {
        if (getType() == TYPE.PREFIX) {
            logger.error("TimeSeriesInterval PREFIX can't not use the getEndTimeSeries func");
            System.exit(0);
        }
        return null;
    }

    default public void setEndTimeSeries(String endTimeSeries) {
        if (getType() == TYPE.PREFIX) {
            logger.error("TimeSeriesInterval PREFIX can't not use the setEndTimeSeries func");
            System.exit(0);
        }
    }

    public String getSchemaPrefix();

    public void setSchemaPrefix(String schemaPrefix);

    public boolean isCompletelyAfter(TimeSeriesInterval tsInterval);

    public boolean isAfter(String tsName);

    public boolean isClosed();

    public void setClosed(boolean closed);

    public static TimeSeriesInterval fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeSeriesIntervalNormal(parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
    }

    public boolean isContain(String tsName);

    public boolean isIntersect(TimeSeriesInterval tsInterval);

    public int compareTo(TimeSeriesInterval o);

}
