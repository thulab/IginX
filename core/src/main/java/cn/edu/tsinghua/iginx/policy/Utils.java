package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;

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

}
