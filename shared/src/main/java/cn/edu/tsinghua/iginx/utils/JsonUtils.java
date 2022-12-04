package cn.edu.tsinghua.iginx.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.charset.StandardCharsets;

public class JsonUtils {

    private static final Gson gson = new GsonBuilder()
            .create();

    public static byte[] toJson(Object o) {
        return gson.toJson(o).getBytes(StandardCharsets.UTF_8);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return gson.fromJson(new String(data), clazz);
    }

}