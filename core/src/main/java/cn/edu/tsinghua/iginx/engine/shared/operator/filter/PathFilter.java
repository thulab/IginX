package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

public class PathFilter implements Filter {

    private final FilterType type = FilterType.Path;

    private final String pathA;
    private final String pathB;
    private Op op;

    public PathFilter(String pathA, Op op, String pathB) {
        this.pathA = pathA;
        this.pathB = pathB;
        this.op = op;
    }

    public void reverseFunc() {
        this.op = Op.getOpposite(op);
    }

    public String getPathA() {
        return pathA;
    }

    public String getPathB() {
        return pathB;
    }

    public Op getOp() {
        return op;
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
        return new PathFilter(pathA, op, pathB);
    }

    @Override
    public String toString() {
        return pathA + " " + Op.op2Str(op) + " " + pathB;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PathFilter that = (PathFilter) o;
        return type == that.type && Objects.equals(pathA, that.pathA) && Objects
            .equals(pathB, that.pathB) && op == that.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, pathA, pathB, op);
    }
}
