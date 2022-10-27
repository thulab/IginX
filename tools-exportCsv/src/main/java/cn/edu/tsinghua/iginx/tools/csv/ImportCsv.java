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
package cn.edu.tsinghua.iginx.tools.csv;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.tools.utils.DataTypeInferenceUtils;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ImportCsv extends AbstractCsvTool {

    private static final String FILE_ARGS = "f";
    private static final String FILE_NAME = "file";

    private static final String DIRECTORY_ARGS = "d";
    private static final String DIRECTORY_NAME = "directory";

    private static final ThreadLocal<SimpleDateFormat> format = new ThreadLocal<>();

    private static String filePaths = "";

    private static String directory = "";

    public static void main(String[] args) {
        Options options = createOptions();

        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        SCRIPT_HINT = "./import_csv.sh(import_csv.bat if Windows)";
        HELP_HINT = String.format("Require more params input, e.g. %n"
            + " ./import_csv.sh(import-csv.bat if Windows) [-h xxx.xxx.xxx.xxx] [-p xxxx] [-u xxx] [-pw xxx] -f xxx");

        if (args == null || args.length == 0) {
            System.out.println(HELP_HINT);
            System.out.println(HINT_STRING);
            hf.printHelp(SCRIPT_HINT, options, true);
            return;
        }

        if (!parseCommandLine(options, args)) {
            return;
        }
        processCommandLine();
    }

    private static Options createOptions() {
        Options options = createCommonOptions();

        options.addOption(FILE_ARGS, FILE_NAME, true, "Import file (optional)");
        options.addOption(DIRECTORY_ARGS, DIRECTORY_NAME, true, "Import directory (optional)");

        return options;
    }

    private static void processCommandLine() {
        parseCommonArgs();
        parseArgs();

        try {
            session = new Session(host, port, username, password);
            session.openSession();

            // 导入文件和导入目录均未指定
            if (filePaths.equals("") && directory.equals("")) {
                System.out.println("[ERROR] Either -f(file to import) or -d(directory to import) must be specified!");
                return;
            }

            // 导入文件和导入目录均指定
            if (!filePaths.equals("") && !directory.equals("")) {
                System.out.println("[ERROR] Only one of -f(file to import) and -d(directory to import) can be specified!");
                return;
            }

            if (!filePaths.equals("")) { // 指定导入文件
                String[] files = filePaths.trim().split(";");
                for (String file : files) {
                    processSingleFile(file);
                }
            } else { // 指定导入目录
                processDirectory();
            }
        } catch (SessionException e) {
            System.out.printf("[ERROR] Encounter an error when opening session, because %s%n", e.getMessage());
        }
    }

    private static void parseArgs() {
        filePaths = parseArg(FILE_ARGS, FILE_NAME, false, "");
        directory = parseArg(DIRECTORY_ARGS, DIRECTORY_NAME, false, "");

        if (!directory.equals("") && !directory.endsWith("/") && !directory.endsWith("\\")) {
            directory += File.separator;
        }

        if (needToParseTime) {
            format.set(new SimpleDateFormat(timeFormat));
        }
    }

    private static void processDirectory() {
        File file = new File(directory);
        if (!file.isDirectory()) {
            System.out.printf("[ERROR] [%s] is not a directory!%n", directory);
            return;
        }

        File[] files = file.listFiles();
        if (files == null) {
            return;
        }

        for (File subFile : files) {
            if (subFile.isFile()) {
                processSingleFile(subFile.getAbsolutePath());
            }
        }
    }

    private static void processSingleFile(String fileName) {
        File file = new File(fileName);
        if (!file.isFile()) {
            System.out.printf("[ERROR] [%s] is not a file!%n", fileName);
            return;
        }
        if (!fileName.endsWith(".csv")) {
            System.out.printf(
                "[ERROR] The file name must end with [csv]! [%s] doesn't satisfy the requirement!%n", fileName);
            return;
        }

        try {
            System.out.printf("Processing file [%s].%n", fileName);
            CSVParser csvParser = CSVFormat.Builder
                .create(CSVFormat.DEFAULT)
                .setHeader()
                .setSkipHeaderRecord(true)
                .setEscape('\\')
                .setQuote('`')
                .setIgnoreEmptyLines(true)
                .build()
                .parse(new InputStreamReader(Files.newInputStream(file.toPath())));

            List<String> headerNames = csvParser.getHeaderNames();
            List<CSVRecord> records = csvParser.getRecords();

            if (!headerNames.get(0).equalsIgnoreCase("time")) {
                System.out.printf(
                    "[ERROR] The first column must be named as [Time]! [%s] doesn't satisfy the requirement!%n", fileName);
                return;
            }

            List<String> paths = new ArrayList<>();
            long[] timestamps = new long[records.size()];
            Object[] valuesList = new Object[records.size()];
            List<DataType> dataTypeList = new ArrayList<>();

            // 填充 paths
            for (int i = 1; i < headerNames.size(); i++) {
                paths.add(headerNames.get(i));
            }

            // 填充 dataTypeList
            Set<Integer> dataTypeIndex = new HashSet<>();
            for (int i = 0; i < paths.size(); i++) {
                dataTypeList.add(null);
            }
            for (int i = 0; i < dataTypeList.size(); i++) {
                dataTypeIndex.add(i);
            }
            for (CSVRecord record : records) {
                for (int j = 1; j < record.size(); j++) {
                    if (!dataTypeIndex.contains(j - 1)) {
                        continue;
                    }
                    DataType inferredDataType = DataTypeInferenceUtils.getInferredDataType(record.get(j));
                    if (inferredDataType != null) { // 找到每一列第一个不为 null 的值进行类型推断
                        dataTypeList.set(j - 1, inferredDataType);
                        dataTypeIndex.remove(j - 1);
                    }
                }
                if (dataTypeIndex.isEmpty()) {
                    break;
                }
            }
            if (!dataTypeIndex.isEmpty()) {
                for (Integer index : dataTypeIndex) {
                    dataTypeList.set(index, DataType.BINARY);
                }
            }

            // 填充 timestamps 和 valuesList
            for (int i = 0; i < records.size(); i++) {
                CSVRecord record = records.get(i);
                if (needToParseTime) {
                    timestamps[i] = format.get().parse(record.get(0)).getTime();
                } else {
                    timestamps[i] = Long.parseLong(record.get(0));
                }
                Object[] values = new Object[record.size() - 1];
                for (int j = 1; j < record.size(); j++) {
                    if (record.get(j).equalsIgnoreCase("null")) {
                        values[j - 1] = null;
                    } else {
                        switch (dataTypeList.get(j - 1)) {
                            case BOOLEAN:
                                values[j - 1] = Boolean.parseBoolean(record.get(j));
                                break;
                            case INTEGER:
                                values[j - 1] = Integer.parseInt(record.get(j));
                                break;
                            case LONG:
                                values[j - 1] = Long.parseLong(record.get(j));
                                break;
                            case FLOAT:
                                values[j - 1] = Float.parseFloat(record.get(j));
                                break;
                            case DOUBLE:
                                values[j - 1] = Double.parseDouble(record.get(j));
                                break;
                            case BINARY:
                                values[j - 1] = record.get(j).getBytes();
                                break;
                            default:
                        }
                    }
                }
                valuesList[i] = values;
            }

            session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
            System.out.printf("Finish to import file [%s].%n", fileName);
        } catch (IOException | SessionException | ExecutionException | ParseException e) {
            System.out.printf(
                "[ERROR] Encounter an error when processing file [%s], because %s%n", fileName, e.getMessage());
        }
    }
}
