package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

// only use for calculate sub filter for now.
public class BoolFilter implements Filter {

    private final FilterType type = FilterType.Bool;

    private final boolean isTrue;

    public BoolFilter(boolean isTrue) {
        this.isTrue = isTrue;
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public void accept(FilterVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public FilterType getType() {
        return type;
    }

    @Override
    public Filter copy() {
        return new BoolFilter(isTrue);
    }

    @Override
    public String toString() {
        return isTrue ? "True" : "False";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BoolFilter that = (BoolFilter) o;
        return isTrue == that.isTrue && type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, isTrue);
    }
}
