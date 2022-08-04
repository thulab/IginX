package cn.edu.tsinghua.iginx.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CurveMatchUtils {

    private static final int maxQuerySize = 256;

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


    public static List<Double> calcShapePattern(List<Double> list, boolean enable, boolean useShape, boolean useAmplitude, double slopeDelta, double concavityDelta) {
        if (!enable) {
            new ArrayList<>().addAll(list);
        }
        List<Double> ret = new ArrayList<>();
        for (int i = 0; i < list.size() - 1; i++) {
            double now = list.get(i + 1) - list.get(i);
            double next = (i == list.size() - 2) ? 0.0 : list.get(i + 2) - list.get(i + 1);
            double ins = useAmplitude ? Math.abs(now) : 1.0;
            if (useShape) {
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
            } else {
                if (now < -slopeDelta) {
                    ins *= -1.0;
                } else if (now < slopeDelta) {
                    ins = 0.0;
                }
            }
            ret.add(ins);
        }
        return ret;
    }

    public static List<Double> norm(List<Double> p) {
        double aver = p.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double std = Math.sqrt(p.stream().mapToDouble(Double::doubleValue).map(e -> Math.pow(e - aver, 2)).average().orElse(1));
        return p.stream().map(e -> (e - aver) / std).collect(Collectors.toList());
    }

    public static double LB_Kim_FL(List<Double> query, List<Double> sequence) {
        double ret = Double.MIN_VALUE;
        int n = query.size();
        ret = Math.max(ret, Math.pow(query.get(0) - sequence.get(0), 2));
        ret = Math.max(ret, Math.pow(query.get(n - 1) - sequence.get(n - 1), 2));
        return ret;
    }

    public static List<Double> getWindow(List<Double> list, int windows, boolean isUpper) {
        List<Double> ret = new ArrayList<>();
        int n = list.size();
        for (int i = 0; i < n; i++) {
            ret.add(list.subList(Math.max(0, i - windows), Math.min(i + windows, n - 1) + 1).stream().mapToDouble(Double::doubleValue).
                reduce(isUpper ? Double::max : Double::min).orElse(isUpper ? Double.MAX_VALUE : Double.MIN_VALUE));
        }
        return ret;
    }

    public static double LB_Keogh(List<Double> sequence, List<Double> upper, List<Double> lower, double nowBest) {
        double ret = 0.0;
        int n = sequence.size();
        for (int i = 0; i < n; i++) {
            if (upper.get(i) < sequence.get(i)) {
                ret += Math.pow(sequence.get(i) - upper.get(i), 2);
            }
            if (lower.get(i) > sequence.get(i)) {
                ret += Math.pow(sequence.get(i) - lower.get(i), 2);
            }
            if (ret > nowBest) {
                return ret;
            }
        }
        return ret;
    }

    public static synchronized double calcDTW(List<Double> query, List<Double> sequence, int maxWarpingWindow, double nowBest, List<Double> upper, List<Double> lower) {
        int n = query.size();
        sequence = norm(sequence);
        if (LB_Kim_FL(query, sequence) > nowBest) {
            return Double.MAX_VALUE;
        }
        double lbk = LB_Keogh(sequence, upper, lower, nowBest);
        if (lbk > nowBest) {
            return Double.MAX_VALUE;
        }

        double[][] dis = new double[maxQuerySize][maxQuerySize];
        dis[0][0] = Math.pow(query.get(0) - sequence.get(0), 2);

        if (upper.get(0) < sequence.get(0)) {
            lbk -= Math.pow(sequence.get(0) - upper.get(0), 2);
        }
        if (lower.get(0) > sequence.get(0)) {
            lbk -= Math.pow(sequence.get(0) - lower.get(0), 2);
        }
        if (lbk + dis[0][0] > nowBest) {
            return lbk + dis[0][0];
        }
        for (int i = 1; i <= maxWarpingWindow; i++) {
            dis[0][i] = dis[0][i-1] + Math.pow(query.get(0) - sequence.get(i), 2);
            dis[i][0] = dis[i-1][0] + Math.pow(query.get(i) - sequence.get(0), 2);
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
                dis[i][j] += Math.pow(query.get(i) - sequence.get(j), 2);
                if (i == j) {
                    if (upper.get(i) < sequence.get(i)) {
                        lbk -= Math.pow(sequence.get(i) - upper.get(i), 2);
                    }
                    if (lower.get(i) > sequence.get(i)) {
                        lbk -= Math.pow(sequence.get(i) - lower.get(i), 2);
                    }
                    if (lbk + dis[i][i] > nowBest) {
                        return lbk + dis[i][i];
                    }
                }
            }
        }
        return dis[n - 1][n - 1];
    }
}
