package cn.edu.tsinghua.iginx.utils;

public class ValueUtils {

    public static double transformToDouble(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Null value");
        }

        if (value instanceof byte[]) {
            return Double.parseDouble(new String((byte[]) value));
        } else if (value instanceof Byte) {
            return Double.parseDouble(new String(new byte[]{(byte) value}));
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        } else if (value instanceof Long) {
            return ((Long) value).doubleValue();
        } else if (value instanceof Integer) {
            return ((Integer) value).doubleValue();
        } else if (value instanceof Short) {
            return ((Short) value).doubleValue();
        } else if (value instanceof Double) {
            return (double) value;
        } else if (value instanceof Float) {
            return ((Float) value).doubleValue();
        } else if (value instanceof Boolean) {
            return ((boolean) value) ? 1.0D : 0.0D;
        } else {
            throw new IllegalArgumentException("Unexpected data type");
        }
    }
}
