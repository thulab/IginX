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

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.QueryDataSet;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.cli.*;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * args[]: -h 127.0.0.1 -p 6888 -u root -pw root
 */
public class IginxClient {

    private static final String IGINX_CLI_PREFIX = "IginX> ";

    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    private static final String EXECUTE_ARGS = "e";
    private static final String EXECUTE_NAME = "execute";

    private static final String HELP_ARGS = "help";

    private static final int MAX_HELP_CONSOLE_WIDTH = 88;

    private static final int MAX_FETCH_SIZE = 1000;

    private static final String SCRIPT_HINT = "./start-cli.sh(start-cli.bat if Windows)";

    private static final String QUIT_COMMAND = "quit";
    private static final String EXIT_COMMAND = "exit";

    static String host = "127.0.0.1";
    static String port = "6888";
    static String username = "root";
    static String password = "root";

    static String execute = "";

    private static int MAX_GETDATA_NUM = 100;
    private static String timestampPrecision = "ns";

    private static CommandLine commandLine;
    private static Session session;

    private static Options createOptions() {
        Options options = new Options();

        options.addOption(HELP_ARGS, false, "Display help information(optional)");
        options.addOption(HOST_ARGS, HOST_NAME, true, "Host Name (optional, default 127.0.0.1)");
        options.addOption(PORT_ARGS, PORT_NAME, true, "Port (optional, default 6888)");
        options.addOption(USERNAME_ARGS, USERNAME_NAME, true, "User name (optional, default \"root\")");
        options.addOption(PASSWORD_ARGS, PASSWORD_NAME, true, "Password (optional, default \"root\")");
        options.addOption(EXECUTE_ARGS, EXECUTE_NAME, true, "Execute (optional)");

        return options;
    }

    private static boolean parseCommandLine(Options options, String[] args, HelpFormatter hf) {
        try {
            CommandLineParser parser = new DefaultParser();
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARGS)) {
                hf.printHelp(SCRIPT_HINT, options, true);
                return false;
            }
        } catch (ParseException e) {
            System.out.println(
                "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
                    + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -pw xxx.");
            System.out.println("For more information, please check the following hint.");
            hf.printHelp(SCRIPT_HINT, options, true);
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        Options options = createOptions();

        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(MAX_HELP_CONSOLE_WIDTH);

        if (args == null || args.length == 0) {
            System.out.println(
                "Require more params input, eg. ./start-cli.sh(start-cli.bat if Windows) "
                    + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -p xxx.");
            System.out.println("For more information, please check the following hint.");
            hf.printHelp(SCRIPT_HINT, options, true);
            return;
        }

        if (!parseCommandLine(options, args, hf)) {
            return;
        }
        serve(args);
    }

    private static String parseArg(String arg, String name, boolean isRequired, String defaultValue) {
        String str = commandLine.getOptionValue(arg);
        if (str == null) {
            if (isRequired && defaultValue == null) {
                String msg =
                    String.format(
                        "%s Required values for option '%s' not provided", IGINX_CLI_PREFIX, name);
                System.out.println(msg);
                System.out.println("Use -help for more information");
                throw new RuntimeException();
            }
            return defaultValue;
        }
        return str;
    }

    private static void serve(String[] args) {
        try {
            Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();

            LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(buildIginxCompleter())
                .build();

            host = parseArg(HOST_ARGS, HOST_NAME, false, "127.0.0.1");
            port = parseArg(PORT_ARGS, PORT_NAME, false, "6888");
            username = parseArg(USERNAME_ARGS, USERNAME_NAME, false, "root");
            password = parseArg(PASSWORD_ARGS, PASSWORD_NAME, false, "root");
            execute = parseArg(EXECUTE_ARGS, EXECUTE_NAME, false, "");

            session = new Session(host, port, username, password);
            session.openSession();

            if (execute.equals("")) {
                echoStarting();
                displayLogo("0.6.0-SNAPSHOT");

                String command;
                while (true) {
                    command = reader.readLine(IGINX_CLI_PREFIX);
                    boolean continues = processCommand(command);
                    if (!continues) {
                        break;
                    }
                }
                System.out.println("Goodbye");
            } else {
                processCommand(parseExecuteCommand(args));
            }
        } catch (UserInterruptException e) {
            System.out.println("Goodbye");
        } catch (RuntimeException e) {
            System.out.println(IGINX_CLI_PREFIX + "Parse Parameter error.");
            System.out.println(IGINX_CLI_PREFIX + "Use -help for more information");
        } catch (Exception e) {
            System.out.println(IGINX_CLI_PREFIX + "exit cli with error " + e.getMessage());
        }
    }

    private static boolean processCommand(String command) {
        if (command == null || command.trim().equals("")) {
            return true;
        }
        String[] cmds = command.trim().split(";");
        for (String cmd : cmds) {
            if (cmd != null && !cmd.trim().equals("")) {
                OperationResult res = handleInputStatement(cmd);
                switch (res) {
                    case STOP:
                        return false;
                    case CONTINUE:
                        continue;
                    default:
                        break;
                }
            }
        }
        return true;
    }

    private static OperationResult handleInputStatement(String statement) {
        String trimedStatement = statement.replaceAll(" +", " ").toLowerCase().trim();

        if (trimedStatement.equals(EXIT_COMMAND) || trimedStatement.equals(QUIT_COMMAND)) {
            return OperationResult.STOP;
        }

        if (isQuery(statement) || isShowTimeSeries(statement)) {
            processSqlWithStream(statement);
        } else {
            processSql(statement);
        }
        return OperationResult.DO_NOTHING;
    }

    private static boolean isQuery(String sql) {
        return sql.startsWith("select");
    }

    private static boolean isShowTimeSeries(String sql) {
        return sql.contains("show") && sql.contains("time") && sql.contains("series");
    }

    private static void processSql(String sql) {
        try {
            SessionExecuteSqlResult res = session.executeSql(sql);

            String parseErrorMsg = res.getParseErrorMsg();
            if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
                System.out.println(res.getParseErrorMsg());
                return;
            }

            if (res.isQuery()) {
                res.print(true, timestampPrecision);
            } else if (res.getSqlType() == SqlType.ShowTimeSeries) {
                res.print(false, "");
            } else if (res.getSqlType() == SqlType.ShowClusterInfo) {
                res.print(false, "");
            } else if (res.getSqlType() == SqlType.ShowRegisterTask) {
                res.print(false, "");
            } else if (res.getSqlType() == SqlType.ShowEligibleJob) {
                res.print(false, "");
            } else if (res.getSqlType() == SqlType.GetReplicaNum) {
                System.out.println(res.getReplicaNum());
                System.out.println("success");
            } else if (res.getSqlType() == SqlType.CountPoints) {
                System.out.println(res.getPointsNum());
                System.out.println("success");
            } else if (res.getSqlType() == SqlType.CommitTransformJob) {
                System.out.println("job id: " + res.getJobId());
            } else if (res.getSqlType() == SqlType.ShowJobStatus) {
                System.out.println("Job status: " + res.getJobState());
            } else {
                System.out.println("success");
            }
        } catch (SessionException | ExecutionException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Execute Error: encounter error(s) when executing sql statement, " +
                "see server log for more details.");
        }
    }

    private static void processSqlWithStream(String sql) {
        try {
            QueryDataSet res = session.executeQuery(sql, MAX_FETCH_SIZE);

            System.out.println("ResultSets:");

            List<List<String>> cache = cacheResult(res);
            System.out.print(FormatUtils.formatResult(cache));

            boolean isCanceled = false;
            int total = cache.size() - 1;

            while (res.hasMore()) {
                System.out.printf(
                    "Reach the max_display_num = %s. Press ENTER to show more, input 'q' to quit.",
                    MAX_FETCH_SIZE);
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                try {
                    if ("".equals(br.readLine())) {
                        cache = cacheResult(res);
                        System.out.print(FormatUtils.formatResult(cache));
                        total += cache.size() - 1;
                    } else {
                        isCanceled = true;
                        break;
                    }
                } catch (IOException e) {
                    System.out.println("IO Error: " + e.getMessage());
                    isCanceled = true;
                    break;
                }
            }
            if (!isCanceled) {
                System.out.print(FormatUtils.formatCount(total));
            }
        } catch (SessionException | ExecutionException e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Execute Error: encounter error(s) when executing sql statement, " +
                "see server log for more details.");
        }
    }

    private static List<List<String>> cacheResult(QueryDataSet queryDataSet) throws ExecutionException, SessionException {
        boolean hasKey = queryDataSet.getColumnList().get(0).equals(GlobalConstant.KEY_NAME);
        List<List<String>> cache = new ArrayList<>();
        cache.add(new ArrayList<>(queryDataSet.getColumnList()));

        int rowIndex = 0;
        while (queryDataSet.hasMore() && rowIndex < MAX_FETCH_SIZE) {
            List<String> strRow = new ArrayList<>();
            Object[] nextRow = queryDataSet.nextRow();
            if (nextRow != null) {
                if (hasKey) {
                    strRow.add(FormatUtils.formatTime((Long) nextRow[0], FormatUtils.DEFAULT_TIME_FORMAT, timestampPrecision));
                    for (int i = 1; i < nextRow.length; i++) {
                        strRow.add(FormatUtils.valueToString(nextRow[i]));
                    }
                } else {
                    Arrays.stream(nextRow).forEach(val -> strRow.add(FormatUtils.valueToString(val)));
                }
                cache.add(strRow);
                rowIndex++;
            }
        }

        return cache;
    }

    private static String parseExecuteCommand(String[] args) {
        StringBuilder command = new StringBuilder();
        int index = 0;
        for (String arg : args) {
            index++;
            if (arg.equals("-" + EXECUTE_ARGS) || arg.equals("-" + EXECUTE_NAME)) {
                break;
            }
        }
        for (int i = index; i < args.length; i++) {
            if (args[i].startsWith("-")) {
                break;
            }
            command.append(args[i]);
            command.append(" ");
        }
        return command.substring(0, command.toString().length() - 1);
    }

    private static Completer buildIginxCompleter() {
        List<Completer> iginxCompleters = new ArrayList<>();

        List<List<String>> withNullCompleters = Arrays.asList(
            Arrays.asList("insert", "into"),
            Arrays.asList("delete", "from"),
            Arrays.asList("delete", "time", "series"),
            Arrays.asList("select"),
            Arrays.asList("add", "storageengine"),
            Arrays.asList("register", "python", "task"),
            Arrays.asList("drop", "python", "task"),
            Arrays.asList("commit", "transform", "job"),
            Arrays.asList("show", "transform", "job", "status"),
            Arrays.asList("cancel", "transform", "job")
        );
        addArgumentCompleters(iginxCompleters, withNullCompleters, true);

        List<List<String>> withoutNullCompleters = Arrays.asList(
            Arrays.asList("show", "replica", "number"),
            Arrays.asList("count", "points"),
            Arrays.asList("clear", "data"),
            Arrays.asList("show", "time", "series"),
            Arrays.asList("show", "cluster", "info"),
            Arrays.asList("show", "register", "python", "task")
        );
        addArgumentCompleters(iginxCompleters, withoutNullCompleters, false);

        List<String> singleCompleters = Arrays.asList("quit", "exit");
        addSingleCompleters(iginxCompleters, singleCompleters);

        Completer iginxCompleter = new AggregateCompleter(iginxCompleters);
        return iginxCompleter;
    }

    private static void addSingleCompleters(List<Completer> iginxCompleters, List<String> completers) {
        for (String keyWord : completers) {
            iginxCompleters.add(new StringsCompleter(keyWord.toLowerCase()));
            iginxCompleters.add(new StringsCompleter(keyWord.toUpperCase()));
        }
    }

    private static void addArgumentCompleters(List<Completer> iginxCompleters, List<List<String>> completers, boolean needNullCompleter) {
        for (List<String> keyWords : completers) {
            List<Completer> upperCompleters = new ArrayList<>();
            List<Completer> lowerCompleters = new ArrayList<>();

            for (String keyWord : keyWords) {
                upperCompleters.add(new StringsCompleter(keyWord.toUpperCase()));
                lowerCompleters.add(new StringsCompleter(keyWord.toLowerCase()));
            }
            if (needNullCompleter) {
                upperCompleters.add(NullCompleter.INSTANCE);
                lowerCompleters.add(NullCompleter.INSTANCE);
            }

            iginxCompleters.add(new ArgumentCompleter(upperCompleters));
            iginxCompleters.add(new ArgumentCompleter(lowerCompleters));
        }
    }

    public static void echoStarting() {
        System.out.println("-----------------------");
        System.out.println("Starting IginX Client");
        System.out.println("-----------------------");
    }

    public static void displayLogo(String version) {
        System.out.println(
            "  _____        _        __   __\n" +
                " |_   _|      (_)       \\ \\ / /\n" +
                "   | |   __ _  _  _ __   \\ V / \n" +
                "   | |  / _` || || '_ \\   > <  \n" +
                "  _| |_| (_| || || | | | / . \\ \n" +
                " |_____|\\__, ||_||_| |_|/_/ \\_\\\n" +
                "         __/ |                 \n" +
                "        |___/                       version " +
                version +
                "\n"
        );
    }

    enum OperationResult {
        STOP,
        CONTINUE,
        DO_NOTHING,
    }
}
