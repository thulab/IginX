package cn.edu.tsinghua.iginx.influxdb.tools;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static long instantToNs(Instant time) {
        return time.getEpochSecond() * 1_000_000_000L + time.getNano();
    }

    public static String nanoTimeToStr(long nanoTime) {
        long remainder = nanoTime % 1_000_000;
        long timeInMs = nanoTime / 1_000_000;
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeInMs), ZoneId.of("UTC")).format(FORMATTER) +
            String.format("%06d", remainder) + 'Z';
    }
}
