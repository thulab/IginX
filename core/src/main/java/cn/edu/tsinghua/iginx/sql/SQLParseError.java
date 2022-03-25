package cn.edu.tsinghua.iginx.sql;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

public class SQLParseError extends BaseErrorListener {

  public static final SQLParseError INSTANCE = new SQLParseError();

  @Override
  public void syntaxError(
      Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int charPositionInLine,
      String msg,
      RecognitionException e) {
    throw new ParseCancellationException(
        "Parse Error: line " + line + ":" + charPositionInLine + " " + msg);
  }
}
