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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.Calendar;

public class TimeUtils {

    /* such as '2011-12-03'. */
    public static final DateTimeFormatter ISO_LOCAL_DATE_WIDTH_1_2;

    static {
        ISO_LOCAL_DATE_WIDTH_1_2 =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
                .appendLiteral('-')
                .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
                .appendLiteral('-')
                .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
                .toFormatter();
    }

    /* such as '2011/12/03'. */
    public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_SLASH;

    static {
        ISO_LOCAL_DATE_WITH_SLASH =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
                .appendLiteral('/')
                .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
                .appendLiteral('/')
                .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
                .toFormatter();
    }

    /* such as '2011.12.03'. */
    public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_DOT;

    static {
        ISO_LOCAL_DATE_WITH_DOT =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NEVER)
                .appendLiteral('.')
                .appendValue(ChronoField.MONTH_OF_YEAR, 1, 2, SignStyle.NEVER)
                .appendLiteral('.')
                .appendValue(ChronoField.DAY_OF_MONTH, 1, 2, SignStyle.NEVER)
                .toFormatter();
    }

    /* such as '10:15:30' or '10:15:30.123'. */
    public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_MS;

    static {
        ISO_LOCAL_TIME_WITH_MS =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendLiteral('.')
                .appendValue(ChronoField.MILLI_OF_SECOND, 3)
                .optionalEnd()
                .toFormatter();
    }

    /* such as '10:15:30' or '10:15:30.123456'. */
    public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_US;

    static {
        ISO_LOCAL_TIME_WITH_US =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendLiteral('.')
                .appendValue(ChronoField.MICRO_OF_SECOND, 6)
                .optionalEnd()
                .toFormatter();
    }

    /* such as '10:15:30' or '10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_NS;

    static {
        ISO_LOCAL_TIME_WITH_NS =
            new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendLiteral('.')
                .appendValue(ChronoField.NANO_OF_SECOND, 9)
                .optionalEnd()
                .toFormatter();
    }

    /* such as '2011-12-03T10:15:30' or '2011-12-03T10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_MS;

    static {
        ISO_DATE_TIME_WITH_MS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WIDTH_1_2)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011-12-03T10:15:30' or '2011-12-03T10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_US;

    static {
        ISO_DATE_TIME_WITH_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WIDTH_1_2)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011-12-03T10:15:30' or '2011-12-03T10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_NS;

    static {
        ISO_DATE_TIME_WITH_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WIDTH_1_2)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    /* such as '2011/12/03T10:15:30' or '2011/12/03T10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH;

    static {
        ISO_DATE_TIME_WITH_SLASH =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011/12/03T10:15:30' or '2011/12/03T10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH_US;

    static {
        ISO_DATE_TIME_WITH_SLASH_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011/12/03T10:15:30' or '2011/12/03T10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH_NS;

    static {
        ISO_DATE_TIME_WITH_SLASH_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    /* such as '2011.12.03T10:15:30' or '2011.12.03T10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT;

    static {
        ISO_DATE_TIME_WITH_DOT =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011.12.03T10:15:30' or '2011.12.03T10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT_US;

    static {
        ISO_DATE_TIME_WITH_DOT_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011.12.03T10:15:30' or '2011.12.03T10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT_NS;

    static {
        ISO_DATE_TIME_WITH_DOT_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    /* such as '2011-12-03 10:15:30' or '2011-12-03 10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SPACE;

    static {
        ISO_DATE_TIME_WITH_SPACE =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011-12-03 10:15:30' or '2011-12-03 10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SPACE_US;

    static {
        ISO_DATE_TIME_WITH_SPACE_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011-12-03 10:15:30' or '2011-12-03 10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SPACE_NS;

    static {
        ISO_DATE_TIME_WITH_SPACE_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    /* such as '2011/12/03 10:15:30' or '2011/12/03 10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH_WITH_SPACE;

    static {
        ISO_DATE_TIME_WITH_SLASH_WITH_SPACE =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011/12/03 10:15:30' or '2011/12/03 10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_US;

    static {
        ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011/12/03 10:15:30' or '2011/12/03 10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_NS;

    static {
        ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    /* such as '2011.12.03 10:15:30' or '2011.12.03 10:15:30.123'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT_WITH_SPACE;

    static {
        ISO_DATE_TIME_WITH_DOT_WITH_SPACE =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .toFormatter();
    }

    /* such as '2011.12.03 10:15:30' or '2011.12.03 10:15:30.123456'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT_WITH_SPACE_US;

    static {
        ISO_DATE_TIME_WITH_DOT_WITH_SPACE_US =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_US)
                .toFormatter();
    }

    /* such as '2011.12.03 10:15:30' or '2011.12.03 10:15:30.123456789'. */
    public static final DateTimeFormatter ISO_DATE_TIME_WITH_DOT_WITH_SPACE_NS;

    static {
        ISO_DATE_TIME_WITH_DOT_WITH_SPACE_NS =
            new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_NS)
                .toFormatter();
    }

    public static final DateTimeFormatter formatter =
        new DateTimeFormatterBuilder()
            .appendOptional(ISO_DATE_TIME_WITH_MS)
            .appendOptional(ISO_DATE_TIME_WITH_US)
            .appendOptional(ISO_DATE_TIME_WITH_NS)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH_US)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH_NS)
            .appendOptional(ISO_DATE_TIME_WITH_DOT)
            .appendOptional(ISO_DATE_TIME_WITH_DOT_US)
            .appendOptional(ISO_DATE_TIME_WITH_DOT_NS)
            .appendOptional(ISO_DATE_TIME_WITH_SPACE)
            .appendOptional(ISO_DATE_TIME_WITH_SPACE_US)
            .appendOptional(ISO_DATE_TIME_WITH_SPACE_NS)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH_WITH_SPACE)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_US)
            .appendOptional(ISO_DATE_TIME_WITH_SLASH_WITH_SPACE_NS)
            .appendOptional(ISO_DATE_TIME_WITH_DOT_WITH_SPACE)
            .appendOptional(ISO_DATE_TIME_WITH_DOT_WITH_SPACE_US)
            .appendOptional(ISO_DATE_TIME_WITH_DOT_WITH_SPACE_NS)
            .toFormatter();

    public static final String DEFAULT_TIMESTAMP_PRECISION = "ns";

    public static long getTimeInMs(long timestamp, String timePrecision) {
        long timeInMs;
        switch (timePrecision) {
            case "s":
                timeInMs = timestamp * 1000;
                break;
            case "us":
                timeInMs = timestamp / 1000;
                break;
            case "ns":
                timeInMs = timestamp / 1000000;
                break;
            default:
                timeInMs = timestamp;
        }
        return timeInMs;
    }

    public static long getTimeInNs(long timestamp, String timePrecision) {
        long timeInNs;
        switch (timePrecision) {
            case "s":
                timeInNs = timestamp * 1000000000;
                break;
            case "ms":
                timeInNs = timestamp * 1000000;
                break;
            case "us":
                timeInNs = timestamp * 1000;
                break;
            case "ns":
                timeInNs = timestamp;
                break;
            default:
                timeInNs = timestamp;
        }
        return timeInNs;
    }

    public static long getTimeFromNsToSpecPrecision(long timestamp, String timePrecision) {
        long time;
        switch (timePrecision) {
            case "s":
                time = timestamp / 1000000000;
                break;
            case "ms":
                time = timestamp / 1000000;
                break;
            case "us":
                time = timestamp / 1000;
                break;
            case "ns":
                time = timestamp;
                break;
            default:
                time = timestamp;
        }
        return time;
    }

    public static long getMicrosecond() {
        long currentTime = System.currentTimeMillis() * 1000;
        long nanoTime = System.nanoTime();
        return currentTime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }

    public static long convertDatetimeStrToLong(String timestampStr) throws ParseException {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr, formatter);
        return LocalDateTime.from(localDateTime).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public static long convertDurationStrToLong(long currentTime, String duration) {
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
                    DEFAULT_TIMESTAMP_PRECISION
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
