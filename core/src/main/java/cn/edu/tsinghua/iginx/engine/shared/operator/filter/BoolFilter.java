package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

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
}
