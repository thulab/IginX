package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;

public class RestUtils {
    private static final int maxQuerySize = 256;
    private static double[][] dis = new double[maxQuerySize][maxQuerySize];

    public static DataType checkType(SessionQueryDataSet sessionQueryDataSet) {
        int n = sessionQueryDataSet.getTimestamps().length;
        int m = sessionQueryDataSet.getPaths().size();
        int ret = 0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                    if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Integer ||
                            sessionQueryDataSet.getValues().get(i).get(j) instanceof Long) {
                        ret = Math.max(ret, 1);
                    } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof Float ||
                            sessionQueryDataSet.getValues().get(i).get(j) instanceof Double) {
                        ret = Math.max(ret, 2);
                    } else if (sessionQueryDataSet.getValues().get(i).get(j) instanceof byte[]) {
                        ret = 3;
                    }
                }
            }
        }
        switch (ret) {
            case 0:
                return DataType.BOOLEAN;
            case 1:
                return DataType.LONG;
            case 2:
                return DataType.DOUBLE;
            case 3:
            default:
                return DataType.BINARY;
        }
    }

    public static long getInterval(long timestamp, long startTime, long duration) {
        return (timestamp - startTime) / duration;
    }

    public static long getIntervalStart(long timestamp, long startTime, long duration) {
        return (timestamp - startTime) / duration * duration + startTime;
    }

    public static List<Double> transToNorm(List<Long> timestamps, List<Double> value, Long unit) {
        List<Double> ret = new ArrayList<>();
        long now = timestamps.get(0);
        int index = 0;
        long last = timestamps.get(timestamps.size() - 1);
        while (now < last) {
            if (now == timestamps.get(index)) {
                ret.add(value.get(index));
            } else {
                ret.add(value.get(index) + (value.get(index + 1) - value.get(index)) / (timestamps.get(index + 1) - timestamps.get(index))
                        * (now - timestamps.get(index)));
            }
            now += unit;
            while (now < last && now >= timestamps.get(index + 1)) {
                index++;
            }
        }
        if (now == last) {
            ret.add(value.get(index));
        }
        return ret;
    }


    public static List<Double> calcShapePattern(List<Double> list, boolean useAmplitude, double slopeDelta, double concavityDelta) {
        List<Double> ret = new ArrayList<>();
        for (int i = 0; i < list.size() - 1; i++) {
            double now = list.get(i + 1) - list.get(i);
            double next = (i == list.size() - 2) ? 0.0 : list.get(i + 2) - list.get(i + 1);
            double ins = useAmplitude ? Math.abs(now) : 1.0;
            if (now < -slopeDelta) {
                if (next < -slopeDelta && Math.abs(next - now) < concavityDelta) {
                    ins *= -2.0;
                } else {
                    ins *= -3.0;
                }
            } else if (now > slopeDelta) {
                if (next > slopeDelta && Math.abs(next - now) < concavityDelta) {
                    ins *= 2.0;
                } else {
                    ins *= 3.0;
                }
            } else {
                ins = 0.0;
            }
            ret.add(ins);
        }
        return ret;
    }

    public static synchronized double calcDTW(List<Double> query, List<Double> sequence, int start, int maxWarpingWindow) {
        int n = query.size();
        if (start + n > sequence.size()) {
            return Double.MAX_VALUE;
        }
        for (int i = 0; i < n; i++) {
            dis[0][i] = Math.abs(query.get(0) - sequence.get(i + start));
            dis[i][0] = Math.abs(query.get(i) - sequence.get(start));
        }
        for (int i = 1; i < n; i++) {
            for (int j = Math.max(1, i - maxWarpingWindow); j < Math.min(n, i + maxWarpingWindow); j++) {
                dis[i][j] = dis[i - 1][j - 1];
                if (i + maxWarpingWindow != j) {
                    dis[i][j] = Math.min(dis[i][j], dis[i - 1][j]);
                }
                if (j + maxWarpingWindow != i) {
                    dis[i][j] = Math.min(dis[i][j], dis[i][j - 1]);
                }
                dis[i][j] += Math.abs(query.get(i) - sequence.get(j + start));
            }
        }
        return dis[n - 1][n - 1];
    }
}
