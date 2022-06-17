package cn.edu.tsinghua.iginx.postgresql.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;

public class TagFilterUtils {

  public static String transformToFilterStr(TagFilter filter) {
    StringBuilder builder = new StringBuilder();
    transformToFilterStr(filter, builder);
    return builder.toString();
  }

  private static void transformToFilterStr(TagFilter filter, StringBuilder builder) {
    switch (filter.getType()) {
      case And:
        AndTagFilter andFilter = (AndTagFilter) filter;
        for (int i = 0; i < andFilter.getChildren().size(); i++) {
          builder.append('(');
          transformToFilterStr(andFilter.getChildren().get(i), builder);
          builder.append(')');
          if (i != andFilter.getChildren().size() - 1) { // 还不是最后一个
            builder.append(" and ");
          }
        }
        break;
      case Or:
        OrTagFilter orFilter = (OrTagFilter) filter;
        for (int i = 0; i < orFilter.getChildren().size(); i++) {
          builder.append('(');
          transformToFilterStr(orFilter.getChildren().get(i), builder);
          builder.append(')');
          if (i != orFilter.getChildren().size() - 1) { // 还不是最后一个
            builder.append(" or ");
          }
        }
        break;
      case Base:
        BaseTagFilter baseFilter = (BaseTagFilter) filter;
        builder.append(baseFilter.getTagKey());
        builder.append("=");
        builder.append(baseFilter.getTagValue());
        break;
    }
  }
}