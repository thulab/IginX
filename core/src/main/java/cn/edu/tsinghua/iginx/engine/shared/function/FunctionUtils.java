package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FunctionUtils {

    private static final Set<String> sysSetToRowFunctionSet = new HashSet<>(
        Arrays.asList("min","max","sum","avg","count", "first_value", "last_value"));

    private static final Set<String> sysSetToSetFunctionSet = new HashSet<>(
        Arrays.asList("first", "last"));

    private final static FunctionManager functionManager = FunctionManager.getInstance();

    public static boolean isRowToRowFunction(String identifier) {
        Function function = functionManager.getFunction(identifier);
        return function.getIdentifier().equals("py_udtf");
    }

    public static boolean isSetToRowFunction(String identifier) {
        if (sysSetToRowFunctionSet.contains(identifier)) {
            return true;
        }
        Function function = functionManager.getFunction(identifier);
        return function.getIdentifier().equals("py_udaf");
    }

    public static boolean isSetToSetFunction(String identifier) {
        if (sysSetToSetFunctionSet.contains(identifier)) {
            return true;
        }
        Function function = functionManager.getFunction(identifier);
        return function.getIdentifier().equals("py_udsf");
    }
}
