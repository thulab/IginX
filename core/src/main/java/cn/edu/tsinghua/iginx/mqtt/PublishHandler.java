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

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;


public class PublishHandler extends AbstractInterceptHandler {

    private static final Logger logger = LoggerFactory.getLogger(PublishHandler.class);

    private final IginxWorker worker = IginxWorker.getInstance();

    private final IPayloadFormatter payloadFormat;

    private final long sessionId;

    public PublishHandler(Config config) {
        payloadFormat = PayloadFormatManager.getInstance().getFormatter(config.getMqttPayloadFormatter());
        // open session as root user
        sessionId = SessionManager.getInstance().openSession(config.getUsername());
    }

    @Override
    public String getID() {
        return "iginx-mqtt-broker-listener";
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        String clientId = msg.getClientID();
        ByteBuf payload = msg.getPayload();
        String topic = msg.getTopicName();
        String username = msg.getUsername();
        MqttQoS qos = msg.getQos();

        logger.debug("Receive publish message. clientId: {}, username: {}, qos = {}, topic: {}, payload: {}",
            clientId, username, qos, topic, payload);

        List<Message> events = payloadFormat.format(payload);
        if (events == null) {
            return;
        }

        // 重排序数据，并过滤空事件
        events = events.stream().filter(Objects::nonNull).sorted((o1, o2) -> {
            if (o1.getKey() != o2.getKey()) {
                return Long.compare(o1.getKey(), o2.getKey());
            }
            return o1.getPath().compareTo(o2.getPath());
        }).collect(Collectors.toList());
        if (events.size() == 0) {
            return;
        }

        // 计算实际写入的数据
        List<String> paths = events.stream().map(Message::getPath).distinct().sorted().collect(Collectors.toList());
        Map<String, DataType> dataTypeMap = new HashMap<>();
        for (Message message : events) {
            if (dataTypeMap.containsKey(message.getPath())) {
                if (dataTypeMap.get(message.getPath()) != message.getDataType()) {
                    logger.error("meet error when process message, data type conflict: {} with type {} and {}", message.getPath(), dataTypeMap.get(message.getPath()), message.getDataType());
                    return;
                }
            } else {
                dataTypeMap.put(message.getPath(), message.getDataType());
            }
        }
        List<DataType> dataTypeList = new ArrayList<>();
        for (String path : paths) {
            dataTypeList.add(dataTypeMap.get(path));
        }

        List<Long> timestamps = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();
        List<ByteBuffer> valuesList = new ArrayList<>();
        int from = 0, to = 0;
        while (from < events.size()) {
            long timestamp = events.get(from).getKey();
            while (to < events.size() && events.get(to).getKey() == timestamp) {
                to++;
            }
            timestamps.add(timestamp);
            Bitmap bitmap = new Bitmap(paths.size());
            Object[] values = new Object[paths.size()];
            for (int i = 0; i < paths.size(); i++) {
                Message event = events.get(from);
                if (event.getPath().equals(paths.get(i))) { // 序列正好匹配上
                    bitmap.mark(i);
                    values[i] = event.getValue();
                    from++;
                } else {
                    values[i] = null;
                }
            }
            bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
            valuesList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
        }

        // 采用行接口写入数据
        InsertNonAlignedRowRecordsReq req = new InsertNonAlignedRowRecordsReq();
        req.setSessionId(sessionId);
        req.setTimestamps(ByteUtils.getColumnByteBuffer(timestamps.toArray(), DataType.LONG));
        req.setPaths(paths);
        req.setDataTypeList(dataTypeList);
        req.setValuesList(valuesList);
        req.setBitmapList(bitmapList);

        Status status = worker.insertNonAlignedRowRecords(req);
        logger.debug("event process result: {}", status);
        msg.getPayload().release();
    }
}
