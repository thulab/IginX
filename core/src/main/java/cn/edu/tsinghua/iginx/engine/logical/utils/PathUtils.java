package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;

public class PathUtils {

    public static final String STAR = "*";

    public static final Character MIN_CHAR = '!';
    public static final Character MAX_CHAR = '~';

    public static TimeSeriesRange trimTimeSeriesInterval(TimeSeriesRange tsInterval) {
        String startPath = tsInterval.getStartTimeSeries();
        if (startPath.contains(STAR)) {
            if (startPath.startsWith(STAR)) {
                startPath = null;
            } else {
                startPath = startPath.substring(0, startPath.indexOf(STAR)) + MIN_CHAR;
            }
        }

        String endPath = tsInterval.getEndTimeSeries();
        if (endPath.contains(STAR)) {
            if (endPath.startsWith(STAR)) {
                endPath = null;
            } else {
                endPath = endPath.substring(0, endPath.indexOf(STAR)) + MAX_CHAR;
            }
        }

        return new TimeSeriesInterval(startPath, endPath);
    }

}
