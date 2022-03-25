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
package cn.edu.tsinghua.iginx.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {

  public static final String DEFAULT_TIMESTAMP_PRECISION = "ms";

  public static long getMicrosecond() {
    long currentTime = System.currentTimeMillis() * 1000;
    long nanoTime = System.nanoTime();
    return currentTime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
  }

  public static long convertDatetimeStrToLong(String timestampStr) throws ParseException {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date;
    try {
      date = format.parse(timestampStr);
    } catch (ParseException e) {
      format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      date = format.parse(timestampStr);
    }
    return date.getTime();
  }

  public static long convertDurationStrToLong(long currentTime, String duration) {
    String timestampPrecision = DEFAULT_TIMESTAMP_PRECISION;
    long total = 0;
    long temp = 0;
    for (int i = 0; i < duration.length(); i++) {
      char ch = duration.charAt(i);
      if (Character.isDigit(ch)) {
        temp *= 10;
        temp += (ch - '0');
      } else {
        String unit = duration.charAt(i) + "";
        if (i + 1 < duration.length() && !Character.isDigit(duration.charAt(i + 1))) {
          i++;
          unit += duration.charAt(i);
        }
        total += TimeUtils.convertDurationStrToLong(
            currentTime == -1 ? -1 : currentTime + total,
            temp,
            unit.toLowerCase(),
            timestampPrecision
        );
        temp = 0;
      }
    }
    return total;
  }

  public static long convertDurationStrToLong(
      long currentTime, long value, String unit, String timestampPrecision) {
    DurationUnit durationUnit = DurationUnit.valueOf(unit);
    long res = value;
    switch (durationUnit) {
      case y:
        res *= 365 * 86_400_000L;
        break;
      case mo:
        if (currentTime == -1) {
          res *= 30 * 86_400_000L;
        } else {
          Calendar calendar = Calendar.getInstance();
          calendar.setTimeInMillis(currentTime);
          calendar.add(Calendar.MONTH, (int) (value));
          res = calendar.getTimeInMillis() - currentTime;
        }
        break;
      case w:
        res *= 7 * 86_400_000L;
        break;
      case d:
        res *= 86_400_000L;
        break;
      case h:
        res *= 3_600_000L;
        break;
      case m:
        res *= 60_000L;
        break;
      case s:
        res *= 1_000L;
        break;
      default:
        break;
    }

    if (timestampPrecision.equals("us")) {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value / 1000;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value;
      } else {
        return res * 1000;
      }
    } else if (timestampPrecision.equals("ns")) {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value * 1000;
      } else {
        return res * 1000_000;
      }
    } else {
      if (unit.equals(DurationUnit.ns.toString())) {
        return value / 1000_000;
      } else if (unit.equals(DurationUnit.us.toString())) {
        return value / 1000;
      } else {
        return res;
      }
    }
  }

  public enum DurationUnit {
    y,
    mo,
    w,
    d,
    h,
    m,
    s,
    ms,
    us,
    ns
  }
}
