package cn.edu.tsinghua.iginx.utils;

import com.alibaba.fastjson2.JSON;
import java.util.HashMap;
import java.util.Map;

public class JsonUtils {

    public static byte[] toJson(Object o) {
        return JSON.toJSONBytes(o);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return JSON.parseObject(data, clazz);
    }

    public static byte[] addType(String type, String typeSpecificName, byte[] data) {
        StringBuilder json = new StringBuilder(new String(data));
        if (json.indexOf(type) != -1) {
            // 3 is the length of the ":{", and the position +1
            json.insert(json.indexOf(type) + type.length() + 3, "\"type\":" + "\"" + typeSpecificName + "\",");
        }
        return json.toString().getBytes();
    }

    public static Map<String, Integer> transform(String content) {
        Map<String, Object> rawMap = JSON.parseObject(content);
        Map<String, Integer> ret = new HashMap<>();
        rawMap.forEach((key, value) -> ret.put(key, (Integer) value));
        return ret;
    }
}