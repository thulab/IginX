/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CurveMatchUtils {

    private static final int maxQuerySize = 256;

    public static List<Double> fetch(List<Long> timestamps, List<Double> value, int startIndex, Long unit, int num) {
        List<Double> ret = new ArrayList<>();
        long now = timestamps.get(startIndex);
        int index = startIndex;
        long last = timestamps.get(timestamps.size() - 1);
        while (now < last) {
            if (now == timestamps.get(index)) {
                ret.add(value.get(index));
            } else {
                ret.add(value.get(index - 1) + (value.get(index) - value.get(index - 1)) / (timestamps.get(index) - timestamps.get(index - 1))
                        * (now - timestamps.get(index - 1)));
            }
            if (ret.size() == num) {
                return ret;
            }
            now += unit;
            while (now < last && index + 1 < timestamps.size() && now >= timestamps.get(index + 1)) {
                index++;
            }
        }
        if (now == last) {
            ret.add(value.get(value.size() - 1));
        }
        return ret;
    }

    public static List<Double> calcShapePattern(List<Double> list, boolean enable, boolean useShape, boolean useAmplitude, double slopeDelta, double concavityDelta) {
        List<Double> ret = new ArrayList<>();
        if (!enable) {
            ret.addAll(list);
        } else {
            for (int i = 0; i < list.size() - 1; i++) {
                double now = list.get(i + 1) - list.get(i);
                double next = (i == list.size() - 2) ? 0.0 : list.get(i + 2) - list.get(i + 1);
                double ins = useAmplitude ? Math.abs(now) : 1.0;
                if (useShape) {
                    if (next < -slopeDelta) {
                        if (now < -slopeDelta) {
                            if (next - now < -concavityDelta) {
                                ins *= -3.0;
                            } else if (next - now > concavityDelta) {
                                ins *= -1.0;
                            } else {
                                ins *= -2.0;
                            }
                        } else {
                            ins *= -3.0;
                        }
                    } else if (next > slopeDelta) {
                        if (now > slopeDelta) {
                            if (next - now < -concavityDelta) {
                                ins *= 1.0;
                            } else if (next - now > concavityDelta) {
                                ins *= 3.0;
                            } else {
                                ins *= 2.0;
                            }
                        } else {
                            ins *= 3.0;
                        }
                    } else {
                        ins = 0.0;
                    }
                } else {
                    if (now < -slopeDelta) {
                        ins *= -1.0;
                    } else if (now > slopeDelta) {
                        ins *= 1.0;
                    } else {
                        ins = 0.0;
                    }
                }
                ret.add(ins);
            }
        }
        return ret;
    }

    public static List<Double> norm(List<Double> p) {
        double aver = p.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double std = Math.sqrt(p.stream().mapToDouble(Double::doubleValue).map(e -> Math.pow(e - aver, 2)).average().orElse(1));
        return p.stream().map(e -> std == 0.0 ? 0.0 : (e - aver) / std).collect(Collectors.toList());
    }

    public static double LB_Kim_FL(List<Double> query, List<Double> sequence) {
        double ret = -Double.MAX_VALUE;
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
                reduce(isUpper ? Double::max : Double::min).orElse(isUpper ? Double.MAX_VALUE : -Double.MAX_VALUE));
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
        double lb = LB_Kim_FL(query, sequence);
        if (lb > nowBest) {
            return Double.MAX_VALUE;
        }
        lb = LB_Keogh(sequence, upper, lower, nowBest);
        if (lb > nowBest) {
            return Double.MAX_VALUE;
        }

        double[][] dis = new double[maxQuerySize][maxQuerySize];
        dis[0][0] = Math.pow(query.get(0) - sequence.get(0), 2);

        if (upper.get(0) < sequence.get(0)) {
            lb -= Math.pow(sequence.get(0) - upper.get(0), 2);
        }
        if (lower.get(0) > sequence.get(0)) {
            lb -= Math.pow(sequence.get(0) - lower.get(0), 2);
        }
        if (lb + dis[0][0] > nowBest) {
            return lb + dis[0][0];
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
                        lb -= Math.pow(sequence.get(i) - upper.get(i), 2);
                    }
                    if (lower.get(i) > sequence.get(i)) {
                        lb -= Math.pow(sequence.get(i) - lower.get(i), 2);
                    }
                    if (lb + dis[i][i] > nowBest) {
                        return lb + dis[i][i];
                    }
                }
            }
        }
        return dis[n - 1][n - 1];
    }
}
