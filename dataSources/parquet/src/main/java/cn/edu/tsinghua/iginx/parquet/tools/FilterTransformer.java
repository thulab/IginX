package cn.edu.tsinghua.iginx.parquet.tools;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.PARQUET_SEPARATOR;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;

import java.util.stream.Collectors;

public class FilterTransformer {

    public static String toString(Filter filter) {
        if (filter == null) {
            return "";
        }
        switch (filter.getType()) {
            case And:
                return toString((AndFilter) filter);
            case Or:
                return toString((OrFilter) filter);
            case Not:
                return toString((NotFilter) filter);
            case Value:
                return toString((ValueFilter) filter);
            case Key:
                return toString((KeyFilter) filter);
            default:
                return "";
        }
    }

    private static String toString(AndFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" and ", "(", ")"));
    }

    private static String toString(OrFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" or ", "(", ")"));
    }

    private static String toString(NotFilter filter) {
        return "not " + filter.toString();
    }

    private static String toString(KeyFilter filter) {
        return "time " + Op.op2Str(filter.getOp()) + " " + filter.getValue();
    }

    private static String toString(ValueFilter filter) {
        if (filter.getOp().equals(Op.LIKE)) {
            return filter.getPath().replace(IGINX_SEPARATOR, PARQUET_SEPARATOR) +
                " regexp '" + filter.getValue().getBinaryVAsString() + "'";
        }
        return filter.getPath().replace(IGINX_SEPARATOR, PARQUET_SEPARATOR) + " " +
            Op.op2Str(filter.getOp()) + " " + filter.getValue().getValue();
    }
}
