package org.apache.zeppelin.iginx;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.FormatUtils;
import org.apache.zeppelin.interpreter.AbstractInterpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.ZeppelinContext;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class IginxInterpreter extends AbstractInterpreter {

    private static final String IGINX_HOST = "iginx.host";
    private static final String IGINX_PORT = "iginx.port";
    private static final String IGINX_USERNAME = "iginx.username";
    private static final String IGINX_PASSWORD = "iginx.password";
    private static final String IGINX_TIME_PRECISION = "iginx.time.precision";

    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final String DEFAULT_PORT = "6888";
    private static final String DEFAULT_USERNAME = "root";
    private static final String DEFAULT_PASSWORD = "root";
    private static final String DEFAULT_TIME_PRECISION = "ns";

    private static final String TAB = "\t";
    private static final String NEWLINE = "\n";
    private static final String WHITESPACE = " ";
    private static final String MULTISPACE = " +";
    private static final String SEMICOLON = ";";

    private String host = "";
    private int port = 0;
    private String username = "";
    private String password = "";
    private String timePrecision = "";
    private Session session;

    private Exception exception;

    public IginxInterpreter(Properties properties) {
        super(properties);
    }

    @Override
    public ZeppelinContext getZeppelinContext() {
        return null;
    }

    @Override
    public void open() throws InterpreterException {
        host = getProperty(IGINX_HOST, DEFAULT_HOST).trim();
        port = Integer.parseInt(getProperty(IGINX_PORT, DEFAULT_PORT).trim());
        username = properties.getProperty(IGINX_USERNAME, DEFAULT_USERNAME).trim();
        password = properties.getProperty(IGINX_PASSWORD, DEFAULT_PASSWORD).trim();
        timePrecision = properties.getProperty(IGINX_TIME_PRECISION, DEFAULT_TIME_PRECISION).trim();

        session = new Session(host, port, username, password);
        try {
            session.openSession();
        } catch (SessionException e) {
            exception = e;
            System.out.println("Can not open session successfully.");
        }
    }

    @Override
    public void close() throws InterpreterException {
        try {
            if (session != null) {
                session.closeSession();
            }
        } catch (SessionException e) {
            exception = e;
            System.out.println("Can not close session successfully.");
        }
    }

    @Override
    public InterpreterResult internalInterpret(String st, InterpreterContext context) throws InterpreterException {
        if (exception != null) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, exception.getMessage());
        }

        String[] cmdList = parseMultiLinesSQL(st);

        InterpreterResult interpreterResult = null;
        for (String cmd : cmdList) {
            interpreterResult = processSql(cmd);
        }

        return interpreterResult;
    }

    private InterpreterResult processSql(String sql) {
        try {
            SessionExecuteSqlResult sqlResult = session.executeSql(sql);

            String parseErrorMsg = sqlResult.getParseErrorMsg();
            if (parseErrorMsg != null && !parseErrorMsg.equals("")) {
                return new InterpreterResult(InterpreterResult.Code.ERROR, sqlResult.getParseErrorMsg());
            }

            String msg = buildResult(sqlResult.getResultInList(
                    true, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision));
            InterpreterResult interpreterResult = new InterpreterResult(InterpreterResult.Code.SUCCESS);
            interpreterResult.add(InterpreterResult.Type.TABLE, msg);
            return interpreterResult;
        } catch (Exception e) {
            return new InterpreterResult(InterpreterResult.Code.ERROR, "encounter error when executing sql statement.");
        }
    }

    private String buildResult(List<List<String>> queryList) {
        StringBuilder builder = new StringBuilder();
        for (List<String> row : queryList) {
            for (String val : row) {
                builder.append(val).append(TAB);
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(NEWLINE);
        }
        return builder.toString();
    }

    private String[] parseMultiLinesSQL(String sql) {
        String[] tmp = sql
                .replace(TAB, WHITESPACE)
                .replace(NEWLINE, WHITESPACE)
                .replaceAll(MULTISPACE, WHITESPACE)
                .toLowerCase().trim()
                .split(SEMICOLON);
        return Arrays.stream(tmp).map(String::trim).toArray(String[]::new);
    }

    @Override
    public void cancel(InterpreterContext context) throws InterpreterException {
        try {
            session.closeSession();
        } catch (SessionException e) {
            exception = e;
            System.out.println("Can not close session successfully.");
        }
    }

    @Override
    public FormType getFormType() throws InterpreterException {
        return FormType.SIMPLE;
    }

    @Override
    public int getProgress(InterpreterContext context) throws InterpreterException {
        return 0;
    }
}
