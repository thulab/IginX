package cn.edu.tsinghua.iginx.conf;

public class ConfigUtils {

    private static final String[] suffixList = new String[] {
            "ms", "s", "m", "h", "min", "hour", "day"
    };

    private static final long[] factors = new long[] {
            1, 1000, 1000 * 60, 1000 * 60 * 60, 1000 * 60, 1000 * 60 * 60, 1000 * 60 * 60 * 24
    };

    /**
     * 将表示时间的字符串以毫秒的形式解析。
     * 支持使用 ms，s，m，h 作为后缀，不含后缀的情况下默认为 ms
     * @param s 表示时间的字符串
     * @return 字符串表示的
     */
    public static long parseTime(String s) {
        long factor = 1;
        int suffixLen = 0;
        for (int i = 0; i < suffixList.length; i++) {
            if (s.endsWith(suffixList[i])) {
                factor = factors[i];
                suffixLen = suffixList[i].length();
                break;
            }
        }
        s = s.substring(0, s.length() - suffixLen);
        long value = Long.parseLong(s);
        return value * factor;
    }

    public static String toTimeString(long time) {
        return Long.toString(time) + "ms";
    }

}
