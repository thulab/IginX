package cn.edu.tsinghua.iginx.migration;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MigrationUtils {

  /**
   * 获取列表的排列组合
   */
  public static <T> List<List<T>> combination(List<T> values, int size) {

    if (0 == size) {
      return Collections.singletonList(Collections.emptyList());
    }

    if (values.isEmpty()) {
      return Collections.emptyList();
    }

    List<List<T>> combination = new LinkedList<>();

    T actual = values.iterator().next();

    List<T> subSet = new LinkedList<T>(values);
    subSet.remove(actual);

    List<List<T>> subSetCombination = combination(subSet, size - 1);

    for (List<T> set : subSetCombination) {
      List<T> newSet = new LinkedList<T>(set);
      newSet.add(0, actual);
      combination.add(newSet);
    }

    combination.addAll(combination(subSet, size));

    return combination;
  }

  public static double mean(Collection<Long> data) {
    return data.stream().mapToLong(Long::longValue).sum() * 1.0 / data.size();
  }

  /**
   * 标准差计算
   */
  public static double variance(Collection<Long> data) {
    double variance = 0;
    for (long dataItem : data) {
      variance = variance + (Math.pow((dataItem - mean(data)), 2));
    }
    variance = variance / (data.size() - 1);
    return Math.sqrt(variance);
  }

}
