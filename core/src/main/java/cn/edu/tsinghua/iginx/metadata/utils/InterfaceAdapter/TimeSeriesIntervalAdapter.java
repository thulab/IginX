package cn.edu.tsinghua.iginx.metadata.utils.InterfaceAdapter;

import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesIntervalInPrefix;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesIntervalNormal;
import com.google.gson.*;
import java.lang.reflect.Type;

public class TimeSeriesIntervalAdapter implements JsonSerializer, JsonDeserializer {

    private static final String CLASSNAME = "CLASSNAME";
    private static final String DATA = "DATA";
    private final Class DEFAULTCLASS = TimeSeriesIntervalNormal.class;

    @Override
    public TimeSeriesInterval deserialize(JsonElement jsonElement, Type type,
                                          JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        TimeSeriesInterval timeSeriesInterval = null;
        if (jsonObject.get("type").toString().equals("\"" + TimeSeriesInterval.TYPE.PREFIX + "\""))
            timeSeriesInterval = jsonDeserializationContext.deserialize(jsonObject, TimeSeriesIntervalInPrefix.class);
        else
            timeSeriesInterval = jsonDeserializationContext.deserialize(jsonObject, DEFAULTCLASS);
        return timeSeriesInterval;
    }

    @Override
    public JsonElement serialize(Object jsonElement, Type type, JsonSerializationContext jsonSerializationContext) {
        return jsonSerializationContext.serialize(jsonElement);
    }

}
