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

import cn.edu.tsinghua.iginx.session.Session;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public abstract class AbstractCsvTool {

    private static final String HOST_ARGS = "h";
    private static final String HOST_NAME = "host";

    private static final String PORT_ARGS = "p";
    private static final String PORT_NAME = "port";

    private static final String USERNAME_ARGS = "u";
    private static final String USERNAME_NAME = "username";

    private static final String PASSWORD_ARGS = "pw";
    private static final String PASSWORD_NAME = "password";

    protected static final String HELP_ARGS = "help";

    protected static final String HINT_STRING = "For more information, please check the following hint.";

    protected static final int MAX_HELP_CONSOLE_WIDTH = 88;

    protected static CommandLine commandLine;
    protected static CommandLineParser parser = new DefaultParser();
    protected static HelpFormatter hf = new HelpFormatter();
    protected static Session session;

    protected static String host = "127.0.0.1";
    protected static String port = "6667";
    protected static String username = "root";
    protected static String password = "root";
    protected static String timestampPrecision = "ms";

    protected static String SCRIPT_HINT;
    protected static String HELP_HINT;

    protected static Options createCommonOptions() {
        Options options = new Options();

        options.addOption(HELP_ARGS, false, "Display help information (optional)");
        options.addOption(HOST_ARGS, HOST_NAME, true, "Host Name (optional, default 127.0.0.1)");
        options.addOption(PORT_ARGS, PORT_NAME, true, "Port (optional, default 6667)");
        options.addOption(USERNAME_ARGS, USERNAME_NAME, true, "User name (optional, default \"root\")");
        options.addOption(PASSWORD_ARGS, PASSWORD_NAME, true, "Password (optional, default \"root\")");

        return options;
    }

    protected static void parseCommonArgs() {
        host = parseArg(HOST_ARGS, HOST_NAME, false, "127.0.0.1");
        port = parseArg(PORT_ARGS, PORT_NAME, false, "6888");
        username = parseArg(USERNAME_ARGS, USERNAME_NAME, false, "root");
        password = parseArg(PASSWORD_ARGS, PASSWORD_NAME, false, "root");
    }

    protected static String parseArg(String arg, String name, boolean isRequired, String defaultValue) {
        String str = commandLine.getOptionValue(arg);
        if (str == null) {
            if (isRequired && defaultValue == null) {
                String msg = String.format("Required values for option '%s' not provided", name);
                System.out.println(msg);
                System.out.println("Use -help for more information");
                throw new RuntimeException();
            }
            return defaultValue;
        }
        return str;
    }

    protected static boolean parseCommandLine(Options options, String[] args) {
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption(HELP_ARGS)) {
                hf.printHelp(SCRIPT_HINT, options, true);
                return false;
            }
        } catch (ParseException e) {
            System.out.println(HELP_HINT);
            System.out.println(HINT_STRING);
            hf.printHelp(SCRIPT_HINT, options, true);
            return false;
        }
        return true;
    }
}
