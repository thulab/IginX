package cn.edu.tsinghua.iginx.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class FormatUtils {

    public static final String DEFAULT_TIME_FORMAT = "default_time_format";

    public static String formatResult(List<List<String>> result) {
        if (result.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        List<Integer> maxSizeList = new ArrayList<>();

        int colCount = result.get(0).size();
        for (int i = 0; i < colCount; i++) {
            maxSizeList.add(0);
        }
        for (List<String> row : result) {
            for (int i = 0; i < colCount; i++) {
                maxSizeList.set(i, Math.max(row.get(i).length(), maxSizeList.get(i)));
            }
        }

        builder.append(buildBlockLine(maxSizeList));
        builder.append(buildRow(result, 0, maxSizeList));
        builder.append(buildBlockLine(maxSizeList));
        for (int i = 1; i < result.size(); i++) {
            builder.append(buildRow(result, i, maxSizeList));
        }
        builder.append(buildBlockLine(maxSizeList));
        return builder.toString();
    }

    private static String buildBlockLine(List<Integer> maxSizeList) {
        StringBuilder blockLine = new StringBuilder();
        for (Integer integer : maxSizeList) {
            blockLine.append("+").append(StringUtils.repeat("-", integer));
        }
        blockLine.append("+").append("\n");
        return blockLine.toString();
    }

    private static String buildRow(List<List<String>> cache, int rowIdx, List<Integer> maxSizeList) {
        StringBuilder builder = new StringBuilder();
        builder.append("|");
        int maxSize;
        String rowValue;
        for (int i = 0; i < maxSizeList.size(); i++) {
            maxSize = maxSizeList.get(i);
            rowValue = cache.get(rowIdx).get(i);
            builder.append(String.format("%" + maxSize + "s|", rowValue));
        }
        builder.append("\n");
        return builder.toString();
    }

    public static String formatCount(int count) {
        if (count <= 0) {
            return "Empty set.\n";
        } else {
            return "Total line number = " + count + "\n";
        }
    }

    public static String formatTime(long timestamp, String timeFormat, String timePrecision) {
        long timeInMs = TimeUtils.getTimeInMs(timestamp, timePrecision);
        if (timeFormat.equals(DEFAULT_TIME_FORMAT)) {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(timeInMs);
        } else {
            return new SimpleDateFormat(timeFormat).format(timeInMs);
        }
    }

    public static String valueToString(Object value) {
        String ret;
        if (value instanceof byte[]) {
            ret = new String((byte[]) value);
        } else {
            ret = String.valueOf(value);
        }
        return ret;
    }
}
