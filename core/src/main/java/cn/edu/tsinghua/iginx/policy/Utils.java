package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;

import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import java.util.*;

public class Utils {

    public static List<String> getPathListFromStatement(DataStatement statement) {
        switch (statement.getType()) {
            case SELECT:
                return new ArrayList<>(((SelectStatement) statement).getPathSet());
            case DELETE:
                return ((DeleteStatement) statement).getPaths();
            case INSERT:
                return ((InsertStatement) statement).getPaths();
        }
        return Collections.emptyList();
    }

    public static List<String> getNonWildCardPaths(List<String> paths) {
        Set<String> beCutPaths = new HashSet<>();
        for (String path: paths) {
            if (!path.contains(Constants.LEVEL_PLACEHOLDER)) {
                beCutPaths.add(path);
                continue;
            }
            String[] parts = path.split("\\" + Constants.LEVEL_SEPARATOR);
            if (parts.length == 0 || parts[0].equals(Constants.LEVEL_PLACEHOLDER)) {
                continue;
            }
            StringBuilder pathBuilder = new StringBuilder();
            for (String part: parts) {
                if (part.equals(Constants.LEVEL_PLACEHOLDER)) {
                    break;
                }
                if (pathBuilder.length() != 0) {
                    pathBuilder.append(Constants.LEVEL_SEPARATOR);
                }
                pathBuilder.append(part);
            }
            beCutPaths.add(pathBuilder.toString());
        }
        return new ArrayList<>(beCutPaths);
    }

    public static TimeInterval getTimeIntervalFromDataStatement(DataStatement statement) {
        StatementType type = statement.getType();
        switch (type) {
            case INSERT:
                InsertStatement insertStatement = (InsertStatement) statement;
                List<Long> times = insertStatement.getTimes();
                return new TimeInterval(times.get(0), times.get(times.size() - 1));
            case SELECT:
                SelectStatement selectStatement = (SelectStatement) statement;
                return new TimeInterval(selectStatement.getStartTime(), selectStatement.getEndTime());
            case DELETE:
                DeleteStatement deleteStatement = (DeleteStatement) statement;
                List<TimeRange> timeRanges = deleteStatement.getTimeRanges();
                long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;
                for (TimeRange timeRange : timeRanges) {
                    if (timeRange.getBeginTime() < startTime) {
                        startTime = timeRange.getBeginTime();
                    }
                    if (timeRange.getEndTime() > endTime) {
                        endTime = timeRange.getEndTime();
                    }
                }
                startTime = startTime == Long.MAX_VALUE ? 0 : startTime;
                endTime = endTime == Long.MIN_VALUE ? Long.MAX_VALUE : endTime;
                return new TimeInterval(startTime, endTime);
            default:
                return new TimeInterval(0, Long.MAX_VALUE);
        }
    }
}
