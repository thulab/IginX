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
package cn.edu.tsinghua.iginx.mqtt;

import cn.edu.tsinghua.iginx.thrift.DataType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonPayloadFormatter implements IPayloadFormatter {

    private static final Logger logger = LoggerFactory.getLogger(JsonPayloadFormatter.class);

    private static final Gson gson = new GsonBuilder().create();

    private static final String JSON_KEY_PATH = "path";
    private static final String JSON_KEY_TIMESTAMP = "timestamp";
    private static final String JSON_KEY_DATATYPE = "dataType";
    private static final String JSON_KEY_VALUE = "value";

    public JsonPayloadFormatter() {
        logger.info("use JsonPayloadFormatter as mqtt message formatter.");
    }

    @Override
    public List<Message> format(ByteBuf payload) {
        if (payload == null) {
            return null;
        }
        String txt = payload.toString(StandardCharsets.UTF_8);
        logger.info("receive message: " + txt);
        JsonArray jsonArray = gson.fromJson(txt, JsonArray.class);
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
            String path = jsonObject.get(JSON_KEY_PATH).getAsString();
            long timestamp = jsonObject.get(JSON_KEY_TIMESTAMP).getAsLong();
            DataType dataType = null;
            Object value = null;
            JsonElement jsonValue = jsonObject.get(JSON_KEY_VALUE);
            switch (jsonObject.get(JSON_KEY_DATATYPE).getAsString()) {
                case "int":
                    dataType = DataType.INTEGER;
                    value = jsonValue.getAsInt();
                    break;
                case "long":
                    dataType = DataType.LONG;
                    value = jsonValue.getAsLong();
                    break;
                case "boolean":
                    dataType = DataType.BOOLEAN;
                    value = jsonValue.getAsBoolean();
                    break;
                case "float":
                    dataType = DataType.FLOAT;
                    value = jsonValue.getAsFloat();
                    break;
                case "double":
                    dataType = DataType.DOUBLE;
                    value = jsonValue.getAsDouble();
                    break;
                case "text":
                    dataType = DataType.BINARY;
                    value = jsonValue.getAsString().getBytes(StandardCharsets.UTF_8);
                    break;
            }
            if (value != null) {
                Message message = new Message();
                message.setPath(path);
                message.setDataType(dataType);
                message.setTimestamp(timestamp);
                message.setValue(value);
                messages.add(message);
            }
        }
        return messages;
    }
}
