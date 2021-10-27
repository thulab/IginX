package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.sql.IginXSqlVisitor;
import cn.edu.tsinghua.iginx.sql.SQLParseError;
import cn.edu.tsinghua.iginx.sql.SqlLexer;
import cn.edu.tsinghua.iginx.sql.SqlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class StatementBuilder {

    private static final StatementBuilder instance = new StatementBuilder();

    private StatementBuilder() {}

    public static StatementBuilder getInstance() {
        return instance;
    }

    public Statement build(String sql) {
        SqlLexer lexer = new SqlLexer(CharStreams.fromString(sql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(SQLParseError.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlParser parser = new SqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SQLParseError.INSTANCE);

        IginXSqlVisitor visitor = new IginXSqlVisitor();
        ParseTree tree = parser.sqlStatement();
        return visitor.visit(tree);
    }
}
