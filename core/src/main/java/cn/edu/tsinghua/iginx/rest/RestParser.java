package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;

import java.util.*;


public class RestParser {
    public Statement parseRest(RequestContext ctx){
        SelectStatement statement=(SelectStatement)ctx.getStatement();
        if(ctx.getSelectTagList()!=null){
            Map<String,List<String>> tagList = ctx.getSelectTagList();
            TagFilter tagFilter = parseAndTagExpression(tagList);
            statement.setTagFilter(tagFilter);
        }
        return statement;
    }

    private TagFilter parseAndTagExpression(Map<String,List<String>> tagList) {
        List<TagFilter> children = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : tagList.entrySet()) {
            children.add(parseOrTagExpression(entry.getKey(),entry.getValue()));
        }
        return new AndTagFilter(children);
    }

    private TagFilter parseOrTagExpression(String tagKey, List<String> tagList) {
        List<TagFilter> children = new ArrayList<>();
        for (String tagValue : tagList) {
            children.add(parseTagExpression(tagKey, tagValue));
        }
        return new OrTagFilter(children);
    }

    private TagFilter parseTagExpression(String tagKey, String tagValue) {
        return new BaseTagFilter(tagKey, tagValue);
    }
}