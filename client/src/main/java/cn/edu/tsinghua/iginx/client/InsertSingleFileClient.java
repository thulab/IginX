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
package cn.edu.tsinghua.iginx.client;

import cn.edu.tsinghua.iginx.exceptions.IginxException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.Double.parseDouble;

public class InsertSingleFileClient {

    private static Session session;

    private static final String separator = "\\s+";

    private static final ThreadLocal<SimpleDateFormat> format = new ThreadLocal<>();

    // 存储组
    private static String storageGroupName;
    // 设备
    private static String deviceId;

    // 飞机型号
    private static String planeType;
    // 架机号
    private static String planeId;
    // 任务单号/任务编号
    private static String taskId;
    // 试飞地点
    private static String place;
    // 试飞日期
    private static String date;
    // 试验类型
    private static String flightType;

    // 采样率
    private static int frequency;

    // 传感器
    private static List<String> measurements;

    public static void main(String[] args) throws SessionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 写入单个文件
        insertSingleFile(args);

        // 关闭 Session
        session.closeSession();
    }

    private static void insertSingleFile(String[] args) {
        if (args.length != 1) {
            System.out.println("参数个数必须为1！");
            return;
        }

        format.set(new SimpleDateFormat("yyyyMMddHH:mm:ss:SSS"));

        String filename = args[0];
        extractInfo(filename);
        try {
            LineIterator it = FileUtils.lineIterator(new File(filename));

            if(!it.hasNext()){
                System.out.println(filename + "是空文件！");
                it.close();
                return;
            }

            String line = it.nextLine();
            extractMeasurements(line);
            int lineNum = 1;

            int batchSize = Math.max(100, Math.min(10000, 512 * 1024 / measurements.size()));

            int cnt = 0;
            int singleCnt = 0;
            long[] timestamps = new long[batchSize];
            Object[] valuesList = new Object[batchSize];

            System.out.println("文件 " + filename + " 参数数量 " + measurements.size() + " 批处理大小 " + batchSize);

            while (it.hasNext()) {
                line = it.nextLine();
                lineNum++;
                String[] contents = line.trim().split(separator);
                long timestamp;
                if (contents[0].length() == 12) {
                    timestamp = format.get().parse(date + contents[0]).getTime() * 1000;
                } else if (contents[0].length() == 15) {
                    timestamp = format.get().parse(date + contents[0].substring(0, 12)).getTime() * 1000 + Long.parseLong(contents[0].substring(12));
                } else {
                    //logger.error("line {} 有非法的时间格式：{}！", lineNum, date + contents[0]);
                    continue;
                }

                //now check whether we need to read another line
                while(it.hasNext() && contents.length < measurements.size() + 1){
//					logger.error("Incomplete line: {}", line);
//					logger.error("Incomplete line with {} elements at line {}", contents.length, lineNum);
                    line = it.nextLine();
                    lineNum++;
                    String[] tempContents = line.trim().split(separator);
                    contents = Arrays.copyOf( contents, contents.length+tempContents.length );
                    for(int i=0;i<tempContents.length;i++)
                        contents[i+contents.length-tempContents.length] = tempContents[i];
                }
                //skip these illegal lines
                if(contents.length > measurements.size() + 1) {
//					logger.error("Skipping illegal line with {} elements: {}", contents.length, line);
//					logger.error("{} : =====================", lineNum);
//					logger.error("Skipping illegal line with {} elements at line {}", contents.length, lineNum);
                    continue;
                }

                timestamps[singleCnt] = timestamp;
                if (measurements.size() != contents.length - 1) {
                    contents = line.trim().split("\t");
                    if (measurements.size() != contents.length - 1) {
//						logger.error("文件{}存在某行数据点数目({})与参数数目({})不符。", filename, contents.length - 1, measurements.size());
                        continue;
                    }
                }
                Object[] tmpValues = new Object[measurements.size()];
                for (int i = 1; i < contents.length; i++) {
                    tmpValues[i - 1] = (isValueValid(contents[i]) ? parseDouble(contents[i]) : 0);
                }
                valuesList[singleCnt] = tmpValues;

                cnt++;
                singleCnt++;
                if (singleCnt % batchSize == 0) {
                    insertBatch(singleCnt, deviceId, measurements, timestamps, valuesList);
                    singleCnt = 0;
                }

                if (cnt % 1_000_000 == 0) {
                    System.out.println("文件 " + filename + " 已加载 " + cnt + " 行");
                }
            }
            if (singleCnt != 0) {
                insertBatch(singleCnt, deviceId, measurements, timestamps, valuesList);
            }
            System.out.println("文件 " + filename + " 内容 " + lineNum + " 行 " + cnt + " 共加载 {} 行。");
            it.close();
        } catch (IOException | ParseException e) {
            System.out.println(e.getMessage());
        }
    }

    private static List<String> extractMeasurements(String line) {
        String myline = fixIllegalParameterChars(line);
        //process the list of measurements
        String[] tempMeasurements = myline.trim().split(separator);
        return new ArrayList<>(Arrays.asList(tempMeasurements).subList(1, tempMeasurements.length));
    }

    private static String fixIllegalParameterChars(String line){
        String ret = line;
        ret = ret.replace("<", "小");
        ret = ret.replace(">", "大");
        ret = ret.replace("=", "等");
        ret = ret.replace(".", "点");
        ret = ret.replace("-", "一");
        return ret;
    }

    private static void extractInfo(String filename) {
        String[] parts = filename.split("-");
        planeType = parts[1];
        planeId = parts[2];
        place = parts[3];
        date = "20" + parts[4];
        flightType = parts[5]+"-"+parts[6];
        flightType = flightType.replace('-', '一');

        String taskIDPostfix = "";
        int pos4TaskId = parts.length - 2;
        try{
            frequency = Integer.parseInt(parts[parts.length - 1].substring(0, parts[parts.length - 1].indexOf(".")));
        }catch(NumberFormatException e){
            frequency = Integer.parseInt(parts[parts.length - 2]);
            taskIDPostfix = parts[parts.length - 1].substring(0, parts[parts.length - 1].indexOf("."));
            pos4TaskId = parts.length - 3;
        }

        //the rest are in the taskID
        taskId="";
        for(int i=7;i<=pos4TaskId;i++)
            taskId = taskId+parts[i]+"-";
        taskId = taskIDPostfix.length()==0?taskId.substring( 0, taskId.length()-1 ):taskId+taskIDPostfix;
        //IoTDB does not support path with these characters
        taskId = taskId.replace('-', '一');
        taskId = taskId.replace('&', '二');

        storageGroupName = "root." + planeType + "." + planeId + "." + taskId;
        deviceId = storageGroupName + "." + place + "." + date + "." + flightType;
    }

    private static boolean isValueValid(String value) {
        try {
            Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    private static void insertBatch(int size, String deviceId, List<String> measurements, long[] timestamps, Object[] values) {
        List<String> pathList = measurements.stream().map(str-> deviceId+"." +str).collect( Collectors.toList() );

        List<DataType> datatypeList = new ArrayList<>();
        for (int i = 0; i < measurements.size(); i++) {
            datatypeList.add(DataType.DOUBLE);
        }

        timestamps = Arrays.copyOfRange(timestamps, 0, size);
        values = Arrays.copyOfRange(values, 0, size);

        try {
            session.insertRowRecords(pathList, timestamps, values, datatypeList, null);
        } catch (IginxException e) {
            System.out.println(e.getMessage());
        }
    }
}
