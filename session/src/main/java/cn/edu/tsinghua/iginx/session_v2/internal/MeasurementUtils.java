package cn.edu.tsinghua.iginx.session_v2.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class MeasurementUtils {

    static List<String> mergeAndSortMeasurements(List<String> measurements) {
        if (measurements.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = measurements.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(measurements);
            return measurements;
        }
        List<String> mergedMeasurements = new ArrayList<>();
        for (String measurement : measurements) {
            if (!measurement.contains("*")) {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (measurement.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
            }
            mergedMeasurements.add(measurement);
        }
        mergedMeasurements.sort(String::compareTo);
        return mergedMeasurements;
    }

}
