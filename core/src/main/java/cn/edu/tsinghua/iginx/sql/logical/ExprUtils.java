package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExprUtils {

    public static Filter toDNF(Filter filter) {
        filter = removeNot(filter);
        filter = removeSingleFilter(filter);
        FilterType type = filter.getType();
        switch (type) {
            case Time:
            case Value:
                return filter;
            case Not:
                throw new SQLParserException("Get DNF failed, filter has not-subFilter.");
            case And:
                return toDNF((AndFilter) filter);
            case Or:
                return toDNF((OrFilter) filter);
            default:
                throw new SQLParserException("Get DNF failed, token type is: " + filter.getType());
        }
    }

    private static Filter toDNF(AndFilter andFilter) {
        List<Filter> children = andFilter.getChildren();
        List<Filter> dnfChildren = new ArrayList<>();
        children.forEach(child -> dnfChildren.add(toDNF(child)));

        boolean childrenWithoutOr = true;
        for (Filter child : dnfChildren) {
            if (child.getType().equals(FilterType.Or)) {
                childrenWithoutOr = false;
                break;
            }
        }

        List<Filter> newChildren = new ArrayList<>();
        if (childrenWithoutOr) {
            dnfChildren.forEach(child -> {
                if (FilterType.isLeafFilter(child.getType())) {
                    newChildren.add(child);
                } else {
                    newChildren.addAll(((AndFilter) child).getChildren());
                }
            });
            return new AndFilter(newChildren);
        } else {
            newChildren.addAll(getConjunctions(dnfChildren));
            return new OrFilter(newChildren);
        }
    }

    private static List<Filter> getConjunctions(List<Filter> filters) {
        List<Filter> cur = getAndChild(filters.get(0));
        for (int i = 1; i < filters.size(); i++) {
            cur = getConjunctions(cur, getAndChild(filters.get(i)));
        }
        return cur;
    }

    private static List<Filter> getConjunctions(List<Filter> first, List<Filter> second) {
        List<Filter> ret = new ArrayList<>();
        for (Filter firstFilter : first) {
            for (Filter secondFilter : second) {
                ret.add(mergeToConjunction(new ArrayList<>(Arrays.asList(firstFilter.copy(), secondFilter.copy()))));
            }
        }
        return ret;
    }

    private static Filter mergeToConjunction(List<Filter> filters) {
        List<Filter> children = new ArrayList<>();
        filters.forEach(child -> {
            if (FilterType.isLeafFilter(child.getType())) {
                children.add(child);
            } else {
                children.addAll(((AndFilter) child).getChildren());
            }
        });
        return new AndFilter(children);
    }

    private static List<Filter> getAndChild(Filter filter) {
        if (filter.getType().equals(FilterType.Or)) {
            return ((OrFilter) filter).getChildren();
        } else {
            return Collections.singletonList(filter);
        }
    }

    private static Filter toDNF(OrFilter orFilter) {
        List<Filter> children = orFilter.getChildren();
        List<Filter> newChildren = new ArrayList<>();
        children.forEach(child -> {
            Filter newChild = toDNF(child);
            if (FilterType.isLeafFilter(newChild.getType()) || newChild.getType().equals(FilterType.And)) {
                newChildren.add(newChild);
            } else {
                newChildren.addAll(((OrFilter) newChild).getChildren());
            }
        });
        return new OrFilter(newChildren);
    }

    private static Filter removeSingleFilter(Filter filter) {
        if (filter.getType().equals(FilterType.Or)) {
            List<Filter> children = ((OrFilter) filter).getChildren();
            children.forEach(child -> child = removeSingleFilter(child));
            return children.size() == 1 ? children.get(0) : filter;
        }
        if (filter.getType().equals(FilterType.And)) {
            List<Filter> children = ((AndFilter) filter).getChildren();
            children.forEach(child -> child = removeSingleFilter(child));
            return children.size() == 1 ? children.get(0) : filter;
        }
        return filter;
    }

    public static Filter removeNot(Filter filter) {
        FilterType type = filter.getType();
        switch (type) {
            case Time:
            case Value:
                return filter;
            case And:
                return removeNot((AndFilter) filter);
            case Or:
                return removeNot((OrFilter) filter);
            case Not:
                return removeNot((NotFilter) filter);
            default:
                throw new SQLParserException(String.format("Unknown token [%s] in reverse filter.", type));
        }
    }

    private static Filter removeNot(AndFilter andFilter) {
        List<Filter> andChildren = andFilter.getChildren();
        for (int i = 0; i < andChildren.size(); i++) {
            Filter childWithoutNot = removeNot(andChildren.get(i));
            andChildren.set(i, childWithoutNot);
        }
        return andFilter;
    }

    private static Filter removeNot(OrFilter orFilter) {
        List<Filter> orChildren = orFilter.getChildren();
        for (int i = 0; i < orChildren.size(); i++) {
            Filter childWithoutNot = removeNot(orChildren.get(i));
            orChildren.set(i, childWithoutNot);
        }
        return orFilter;
    }

    private static Filter removeNot(NotFilter notFilter) {
        return reverseFilter(notFilter.getChild());
    }

    private static Filter reverseFilter(Filter filter) {
        if (filter == null)
            return null;

        FilterType type = filter.getType();
        switch (filter.getType()) {
            case Time:
                ((TimeFilter) filter).reverseFunc();
                return filter;
            case Value:
                ((ValueFilter) filter).reverseFunc();
                return filter;
            case And:
                List<Filter> andChildren = ((AndFilter) filter).getChildren();
                for (int i = 0; i < andChildren.size(); i++) {
                    Filter childWithoutNot = reverseFilter(andChildren.get(i));
                    andChildren.set(i, childWithoutNot);
                }
                return new OrFilter(andChildren);
            case Or:
                List<Filter> orChildren = ((OrFilter) filter).getChildren();
                for (int i = 0; i < orChildren.size(); i++) {
                    Filter childWithoutNot = reverseFilter(orChildren.get(i));
                    orChildren.set(i, childWithoutNot);
                }
                return new AndFilter(orChildren);
            case Not:
                return removeNot(((NotFilter) filter).getChild());
            default:
                throw new SQLParserException(String.format("Unknown token [%s] in reverse filter.", type));
        }
    }

    public static List<TimeRange> getTimeRangesFromFilter(Filter filter) {
        filter = toDNF(filter);
        List<TimeRange> timeRanges = new ArrayList<>();
        extractTimeRange(timeRanges, filter);
        return unionTimeRanges(timeRanges);
    }

    private static void extractTimeRange(List<TimeRange> timeRanges, Filter f) {
        FilterType type = f.getType();
        switch (type) {
            case Time:
                timeRanges.add(getTimeRangesFromTimeFilter((TimeFilter) f));
                break;
            case And:
                TimeRange range = getTimeRangeFromAndFilter((AndFilter) f);
                if (range != null)
                    timeRanges.add(range);
                break;
            case Or:
                List<TimeRange> ranges = getTimeRangeFromOrFilter((OrFilter) f);
                if (ranges != null && !ranges.isEmpty()) {
                    timeRanges.addAll(ranges);
                }
                break;
            default:
                throw new SQLParserException(String.format("Illegal token [%s] in getTimeRangeFromAndFilter.", type));
        }
    }

    private static List<TimeRange> getTimeRangeFromOrFilter(OrFilter filter) {
        List<TimeRange> timeRanges = new ArrayList<>();
        filter.getChildren().forEach(f -> extractTimeRange(timeRanges, f));
        return unionTimeRanges(timeRanges);
    }

    private static TimeRange getTimeRangeFromAndFilter(AndFilter filter) {
        List<TimeRange> timeRanges = new ArrayList<>();
        filter.getChildren().forEach(f -> extractTimeRange(timeRanges, f));
        return intersectTimeRanges(timeRanges);
    }

    private static TimeRange getTimeRangesFromTimeFilter(TimeFilter filter) {
        switch (filter.getOp()) {
            case L:
                return new TimeRange(0, filter.getValue());
            case LE:
                return new TimeRange(0, filter.getValue() + 1);
            case G:
                return new TimeRange(filter.getValue() + 1, Long.MAX_VALUE);
            case GE:
                return new TimeRange(filter.getValue(), Long.MAX_VALUE);
            case E:
                return new TimeRange(filter.getValue(), filter.getValue() + 1);
            case NE:
                throw new SQLParserException("Not support [!=] in delete clause.");
            default:
                throw new SQLParserException(String.format("Unknown op [%s] in getTimeRangeFromTimeFilter.", filter.getOp()));
        }
    }

    private static List<TimeRange> unionTimeRanges(List<TimeRange> timeRanges) {
        if (timeRanges == null || timeRanges.isEmpty())
            return new ArrayList<>();
        timeRanges.sort((tr1, tr2) -> {
            long diff = tr1.getBeginTime() - tr2.getBeginTime();
            return diff == 0 ? 0 : diff > 0 ? 1 : -1;
        });

        List<TimeRange> res = new ArrayList<>();

        TimeRange cur = timeRanges.get(0);
        for (int i = 1; i < timeRanges.size(); i++) {
            TimeRange union = unionTwoTimeRanges(cur, timeRanges.get(i));
            if (union == null) {
                res.add(cur);
                cur = timeRanges.get(i);
            } else {
                cur = union;
            }
        }
        res.add(cur);
        return res;
    }

    private static TimeRange unionTwoTimeRanges(TimeRange first, TimeRange second) {
        if (first.getEndTime() < second.getBeginTime() || first.getBeginTime() > second.getEndTime()) {
            return null;
        }
        long begin = Math.min(first.getBeginTime(), second.getBeginTime());
        long end = Math.max(first.getEndTime(), second.getEndTime());
        return new TimeRange(begin, end);
    }

    private static TimeRange intersectTimeRanges(List<TimeRange> timeRanges) {
        if (timeRanges == null || timeRanges.isEmpty())
            return null;
        TimeRange ret = timeRanges.get(0);
        for (int i = 1; i < timeRanges.size(); i++) {
            ret = intersectTwoTimeRanges(ret, timeRanges.get(i));
        }
        return ret;
    }

    private static TimeRange intersectTwoTimeRanges(TimeRange first, TimeRange second) {
        if (first.getEndTime() < second.getBeginTime() || first.getBeginTime() > second.getEndTime()) {
            return null;
        }
        long begin = Math.max(first.getBeginTime(), second.getBeginTime());
        long end = Math.min(first.getEndTime(), second.getEndTime());
        return new TimeRange(begin, end);
    }
}
