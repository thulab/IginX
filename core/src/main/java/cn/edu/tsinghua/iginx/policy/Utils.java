package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import java.util.ArrayList;
import java.util.List;

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
    return null;
  }
}
