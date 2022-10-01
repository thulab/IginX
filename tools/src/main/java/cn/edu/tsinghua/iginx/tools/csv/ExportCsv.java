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
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class ExportCsv extends AbstractCsvTool {

    private static final String DIRECTORY_ARGS = "d";
    private static final String DIRECTORY_NAME = "directory";

    private static final String QUERY_STATEMENT_ARGS = "q";
    private static final String QUERY_STATEMENT_NAME = "query";

    private static final String SQL_FILE_ARGS = "s";
    private static final String SQL_FILE_NAME = "sql";

    private static final String TIME_PRECISION_ARGS = "tp";
    private static final String TIME_PRECISION_NAME = "precision";

    private static final String EXPORT_FILE_NAME_PREFIX = "export";

    private static String directory = "";

    private static String queryStatements = "";

    private static String sqlFile = "";
    private static String timePrecision = "";

    public static void main(String[] args) {
        Options options = createOptions();

        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);
        SCRIPT_HINT = "./export_csv.sh(export_csv.bat if Windows)";
        HELP_HINT = String.format("Require more params input, e.g. %n"
            + " ./export_csv.sh(export_csv.bat if Windows) [-h xxx.xxx.xxx.xxx] [-p xxxx] [-u xxx] [-pw xxx] [-d xxx] -q xxx %n"
            + " or %n"
            + " ./export_csv.sh(export_csv.bat if Windows) [-h xxx.xxx.xxx.xxx] [-p xxxx] [-u xxx] [-pw xxx] [-d xxx] -s xxx");

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

        options.addOption(DIRECTORY_ARGS, DIRECTORY_NAME, true, "Export directory (optional, default current directory)");
        options.addOption(QUERY_STATEMENT_ARGS, QUERY_STATEMENT_NAME, true, "Query statement to export (optional)");
        options.addOption(SQL_FILE_ARGS, SQL_FILE_NAME, true, "SQL file to export (optional)");
        options.addOption(TIME_PRECISION_ARGS, TIME_PRECISION_NAME, true, "Time precision (optional, default \"ms\")");

        return options;
    }

    private static void processCommandLine() {
        parseCommonArgs();
        parseArgs();

        // 查询语句和查询文件均未指定
        if (queryStatements.equals("") && sqlFile.equals("")) {
            System.out.println("[ERROR] Either -q(query statement to export) or -s(sql file to export) must be specified!");
            return;
        }

        // 查询语句和查询文件均指定
        if (!queryStatements.equals("") && !sqlFile.equals("")) {
            System.out.println("[ERROR] Only one of -q(query statement to export) and -s(sql file to export) can be specified!");
            return;
        }

        try {
            session = new Session(host, port, username, password);
            session.openSession();

            if (!queryStatements.equals("")) { // 指定查询语句
                String[] statements = queryStatements.trim().split(";");
                for (int i = 0; i < statements.length; i++) {
                    processSql(statements[i], i);
                }
            } else { // 指定查询文件
                processSqlFile();
            }
        } catch (SessionException e) {
            System.out.printf("[ERROR] Encounter an error when opening session, because %s%n", e.getMessage());
        } catch (IOException e) {
            System.out.printf("[ERROR] Encounter an error when opening sql file [%s], because %s%n", sqlFile, e.getMessage());
        }
    }

    private static void parseArgs() {
        directory = parseArg(DIRECTORY_ARGS, DIRECTORY_NAME, false, "");
        queryStatements = parseArg(QUERY_STATEMENT_ARGS, QUERY_STATEMENT_NAME, false, "");
        sqlFile = parseArg(SQL_FILE_ARGS, SQL_FILE_NAME, false, "");
        timePrecision = parseArg(TIME_PRECISION_ARGS, TIME_PRECISION_NAME, false, "ns");

        if (!directory.equals("") && !directory.endsWith("/") && !directory.endsWith("\\")) {
            directory += File.separator;
        }
    }

    private static void processSqlFile() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(sqlFile))) {
            String sql;
            int index = 0;
            while ((sql = reader.readLine()) != null) {
                processSql(sql, index);
                index++;
            }
        }
    }

    private static void processSql(String sql, int index) {
        try {
            SessionExecuteSqlResult res = session.executeSql(sql);
            String filePath = directory + EXPORT_FILE_NAME_PREFIX + index + ".csv";
            String parseErrorMsg = res.getParseErrorMsg();
            if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
                System.out.println(res.getParseErrorMsg());
                return;
            }

            if (res.isQuery()) {
                System.out.printf("Processing sql statement [%s].%n", sql);
                writeCsvFile(res, filePath);
                System.out.printf("Finish to export file [%s].%n", filePath);
            } else {
                System.out.printf(
                    "[ERROR] Non-query sql statement [%s] is not supported.%n", sql);
            }
        } catch (SessionException | ExecutionException e) {
            System.out.printf(
                "[ERROR] Encounter an error when executing sql statement [%s], because %s%n",
                sql, e.getMessage());
        }
    }

    private static void writeCsvFile(SessionExecuteSqlResult res, String filePath) {
        try {
            CSVPrinter printer = CSVFormat.Builder
                .create(CSVFormat.DEFAULT)
                .setHeader()
                .setSkipHeaderRecord(true)
                .setEscape('\\')
                .setQuoteMode(QuoteMode.NONE)
                .build()
                .print(new PrintWriter(filePath));

            printer.printRecords(res.getResultInList(needToParseTime, timeFormat, timePrecision));

            printer.flush();
            printer.close();
        } catch (IOException e) {
            System.out.printf(
                "[ERROR] Encounter an error when writing csv file [%s], because %s%n",
                filePath, e.getMessage());
        }
    }
}
