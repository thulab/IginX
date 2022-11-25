package cn.edu.tsinghua.iginx.sql.statement.join;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class JoinPart {

    private final String pathPrefix;

    private final JoinType joinType;

    private final Filter filter;

    private final List<String> joinColumns;

    public JoinPart(String pathPrefix) {
        this(pathPrefix, JoinType.CrossJoin, null, Collections.emptyList());
    }

    public JoinPart(String pathPrefix, JoinType joinType, Filter filter, List<String> joinColumns) {
        this.pathPrefix = pathPrefix;
        this.joinType = joinType;
        this.filter = filter;
        this.joinColumns = joinColumns;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Filter getFilter() {
        return filter;
    }

    public List<String> getJoinColumns() {
        return joinColumns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinPart joinPart = (JoinPart) o;
        return Objects.equals(pathPrefix, joinPart.pathPrefix)
            && joinType == joinPart.joinType && Objects.equals(filter, joinPart.filter)
            && Objects.equals(joinColumns, joinPart.joinColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pathPrefix, joinType, filter, joinColumns);
    }
}
