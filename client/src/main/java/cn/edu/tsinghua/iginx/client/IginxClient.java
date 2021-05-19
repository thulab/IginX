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

import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/** args[]: -h 127.0.0.1 -p 6667 -u root -pw root */
public class IginxClient {

    private static final String IGINX_CLI_PREFIX = "IginX";

    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    private static final String HELP_ARGS = "help";

    private static final int MAX_HELP_CONSOLE_WIDTH = 88;

    private static final String SCRIPT_HINT = "./start-cli.sh(start-cli.bat if Windows)";

    static String host = "127.0.0.1";

    static String port = "6667";

    static String username = "root";

    static String password = "root";

    private static CommandLine commandLine;

    private static Session session;

    private static Options createOptions() {
        Options options = new Options();

        options.addOption(HELP_ARGS, false, "Display help information(optional)");
        options.addOption(HOST_ARGS, HOST_NAME, true, "Host Name (optional, default 127.0.0.1)");
        options.addOption(PORT_ARGS, PORT_NAME, true, "Port (optional, default 6667)");
        options.addOption(USERNAME_ARGS, USERNAME_NAME, true, "User name (optional, default \"root\")");
        options.addOption(PASSWORD_ARGS, PASSWORD_NAME, true, "Password (optional, default \"root\")");

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
                            + "-h xxx.xxx.xxx.xxx -p xxxx -u xxx -p xxx.");
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
        serve();
    }

    private static String parseArg(String arg, String name, boolean isRequired, String defaultValue) {
        String str = commandLine.getOptionValue(arg);
        if (str == null) {
            if (isRequired && defaultValue == null) {
                String msg =
                        String.format(
                                "%s> Required values for option '%s' not provided", IGINX_CLI_PREFIX, name);
                System.out.println(msg);
                System.out.println("Use -help for more information");
                throw new RuntimeException();
            }
            return defaultValue;
        }
        return str;
    }

    private static void serve() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            host = parseArg(HOST_ARGS, HOST_NAME, false, "127.0.0.1");
            port = parseArg(PORT_ARGS, PORT_NAME, false, "6324");
            username = parseArg(USERNAME_ARGS, USERNAME_NAME, false, "root");
            password = parseArg(PASSWORD_ARGS, PASSWORD_NAME, false, "root");

            session = new Session(host, port, username, password);
            session.openSession();

            System.out.print(IGINX_CLI_PREFIX + "> ");
            String command;
            while (!(command = reader.readLine()).equals("quit")) {
                processCommand(command);
                System.out.print(IGINX_CLI_PREFIX + "> ");
            }
            System.out.println("Goodbye");
        } catch (RuntimeException e) {
            System.out.println(IGINX_CLI_PREFIX + "> Parse Parameter error.");
            System.out.println(IGINX_CLI_PREFIX + "> Use -help for more information");
        } catch (Exception e) {
            System.out.println(IGINX_CLI_PREFIX + "> exit cli with error " + e.getMessage());
        }
    }

    private static void processCommand(String command) {
        String[] commandParts = command.split(" ");
        if (commandParts.length < 3 || !commandParts[0].equals("add") || !commandParts[1].equals("storageEngine")) {
            System.out.println("unsupported command");
            return;
        }
        String[] storageEngineParts = commandParts[2].split("#");
        String ip = storageEngineParts[0];
        int port = Integer.parseInt(storageEngineParts[1]);
        StorageEngineType storageEngineType = StorageEngine.toThrift(StorageEngine.fromString(storageEngineParts[2]));
        Map<String, String> extraParams = new HashMap<>();
        for (int i = 3; i < storageEngineParts.length; i++) {
            String[] KAndV = storageEngineParts[i].split("=");
            if (KAndV.length != 2) {
                System.out.println("unexpected storage engine meta info: " + storageEngineParts[i]);
                continue;
            }
            extraParams.put(KAndV[0], KAndV[1]);
        }
        try {
            session.addStorageEngine(ip, port, storageEngineType, extraParams);
            System.out.println("success");
        } catch (Exception e) {
            System.out.println("encounter error when add storage engine, please check the status of storage engine.");
        }
    }


}
