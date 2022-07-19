package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.thrift.*;

import java.util.*;


public class RestParser {

    public void parseRest(SelectStatement statement, QueryDataReq req){
        for(String path : req.getPaths())
            statement.setFromPath(path);
        if(req.getTagsList().size() != 0){
            Map<String,List<String>> tagList = req.getTagsList();
            TagFilter tagFilter = parseAndTagExpression(tagList);
            statement.setTagFilter(tagFilter);
        }
        if(req.getAggregatorType() != null){
            statement.setHasFunc(true);
            statement.setHasValueFilter(false);
            //因为初始化时加入了一个空path需要清空，首先清空这些数据
            statement.setSelectedFuncsAndPaths(new HashMap<>());
            statement.setSelectedFuncsAndPaths(req.getAggregatorType(), "");
            switch (req.getAggregatorType()) { 
                case "AVG": 
                    // parseGroupByTimeClause(statement, req);
                    break;
                default: 
            }
        }
        statement.setQueryType();
    }

    // private String parseDurUnit(String dur){
    //     String ret;
    //     List<String> tiemUnit = Arrays.asList(
    //         "months", "mo", 
    //         "weeks", "w", 
    //         "days", "d", 
    //         "hours", "h", 
    //         "seconds", "s", 
    //         "milliseconds", "ms"
    //         );
    //     for(int i=0;i<=12;i+=2){
    //         if(i==12){
    //             //出错！
    //             return null;
    //         }
    //         int pos=dur.indexOf(tiemUnit[i]);
    //         if(pos!=-1){
    //             ret = dur.substr(pos).get(0);
    //             ret += tiemUnit[i+1];
    //             return ret;
    //         }
    //     }
    // }

    private void parseGroupByTimeClause(SelectStatement statement, QueryDataReq req) {
        long precision = req.getDur();
            
        //开始结束时间点
        statement.setStartTime(req.getStartTime());
        statement.setEndTime(req.getEndTime());

        //设置长度
        statement.setPrecision(precision);
        statement.setHasGroupByTime(true);

        // merge value filter and group time range filter
        TimeFilter startTime = new TimeFilter(Op.GE, req.getStartTime());
        TimeFilter endTime = new TimeFilter(Op.L, req.getEndTime());
        Filter mergedFilter;
        if (statement.hasValueFilter()) {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(statement.getFilter(), startTime, endTime)));
        } else {
            mergedFilter = new AndFilter(new ArrayList<>(Arrays.asList(startTime, endTime)));
            statement.setHasValueFilter(true);
        }
        statement.setFilter(mergedFilter);
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