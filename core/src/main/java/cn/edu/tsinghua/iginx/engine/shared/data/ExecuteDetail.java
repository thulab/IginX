package cn.edu.tsinghua.iginx.engine.shared.data;

import java.util.List;

public class ExecuteDetail {

  private final List<Boolean> resultList;
  private final List<String> failureDetails;

  public ExecuteDetail(List<Boolean> resultList, List<String> failureDetails) {
    this.resultList = resultList;
    this.failureDetails = failureDetails;
  }

  public boolean isFullSuccess() {
    for (Boolean success : resultList) {
      if (!success) {
        return false;
      }
    }
    return true;
  }

  public boolean isFullFailure() {
    for (Boolean success : resultList) {
      if (success) {
        return false;
      }
    }
    return true;
  }

  public List<String> getFailureDetails() {
    return failureDetails;
  }
}
