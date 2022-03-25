package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class TestUtils {

  public static Statement buildStatement(String sql) {
    SqlLexer lexer = new SqlLexer(CharStreams.fromString(sql));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SqlParser parser = new SqlParser(tokens);
    IginXSqlVisitor visitor = new IginXSqlVisitor();
    ParseTree tree = parser.sqlStatement();
    return visitor.visit(tree);
  }

  public static void print(Operator root) {
    printOperator(root, "\t");
  }

  private static void printOperator(Operator operator, String start) {
    if (operator == null) {
      return;
    }
    String mid = start.substring(0, start.lastIndexOf("\t")) + "└---";
    System.out.println(mid + operator.getType());
    if (operator instanceof UnaryOperator) {
      UnaryOperator uNode = (UnaryOperator) operator;
      printSource(uNode.getSource(), start + "\t");
    } else if (operator instanceof BinaryOperator) {
      BinaryOperator bNode = (BinaryOperator) operator;
      printSource(bNode.getSourceA(), start + "\t");
      printSource(bNode.getSourceB(), start + "\t");
    } else if (operator instanceof MultipleOperator) {
      MultipleOperator mNode = (MultipleOperator) operator;
      for (Source source : mNode.getSources()) {
        printSource(source, start + "\t");
      }
    }
  }

  private static void printSource(Source source, String start) {
    if (source instanceof OperatorSource) {
      OperatorSource opSource = (OperatorSource) source;
      printOperator(opSource.getOperator(), start);
    } else {
      String mid = start.substring(0, start.lastIndexOf("\t")) + "└---";
      if (source instanceof FragmentSource) {
        FragmentSource fSource = (FragmentSource) source;
        System.out.println(mid + formatFragment(fSource.getFragment()));
      } else if (source instanceof GlobalSource) {
        System.out.println(mid + "global");
      }
    }
  }

  private static String formatFragment(FragmentMeta fragment) {
    return "time[" +
        fragment.getTimeInterval().getStartTime() + ", " +
        fragment.getTimeInterval().getEndTime() + "), " +
        "ts[" +
        fragment.getTsInterval().getStartTimeSeries() + ", " +
        fragment.getTsInterval().getEndTimeSeries() + ")";
  }
}
