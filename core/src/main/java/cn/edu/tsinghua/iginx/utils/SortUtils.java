package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class SortUtils {

    public static InsertColumnRecordsReq sortAndBuildInsertReq(long sessionId, List<String> paths, long[] timestamps, Object[] valuesList,
                                                               List<DataType> dataTypeList, List<Map<String, String>> attributesList) {
        if (paths.isEmpty() || timestamps.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
            return null;
        }
        if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
            return null;
        }
        if (attributesList != null && paths.size() != attributesList.size()) {
            return null;
        }

        Integer[] index = new Integer[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(timestamps))::get));
        Arrays.sort(timestamps);
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = new Object[index.length];
            for (int j = 0; j < index.length; j++) {
                values[j] = ((Object[]) valuesList[i])[index[j]];
            }
            valuesList[i] = values;
        }

        index = new Integer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparing(paths::get));
        Collections.sort(paths);
        Object[] sortedValuesList = new Object[valuesList.length];
        List<DataType> sortedDataTypeList = new ArrayList<>();
        List<Map<String, String>> sortedAttributesList = new ArrayList<>();
        for (int i = 0; i < valuesList.length; i++) {
            sortedValuesList[i] = valuesList[index[i]];
            sortedDataTypeList.add(dataTypeList.get(index[i]));
        }
        if (attributesList != null) {
            for (Integer i : index) {
                sortedAttributesList.add(attributesList.get(i));
            }
        }

        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < sortedValuesList.length; i++) {
            Object[] values = (Object[]) sortedValuesList[i];
            if (values.length != timestamps.length) {
                return null;
            }
            valueBufferList.add(ByteUtils.getColumnByteBuffer(values, sortedDataTypeList.get(i)));
            Bitmap bitmap = new Bitmap(timestamps.length);
            for (int j = 0; j < timestamps.length; j++) {
                if (values[j] != null) {
                    bitmap.mark(j);
                }
            }
            bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
        }

        InsertColumnRecordsReq req = new InsertColumnRecordsReq();
        req.setSessionId(sessionId);
        req.setPaths(paths);
        req.setTimestamps(getByteArrayFromLongArray(timestamps));
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(sortedDataTypeList);
        req.setAttributesList(sortedAttributesList);

        return req;
    }

    // 适用于查询类请求和删除类请求，因为其 paths 可能带有 *
    public static List<String> mergeAndSortPaths(List<String> paths) {
        if (paths.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = paths.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(paths);
            return paths;
        }
        List<String> mergedPaths = new ArrayList<>();
        for (String path : paths) {
            if (path.contains("*")) {
                mergedPaths.add(path);
            } else {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (path.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
                mergedPaths.add(path);
            }
        }
        mergedPaths.sort(Comparator.comparing(o -> o.substring(0, o.indexOf("*"))));
        return mergedPaths;
    }
}
