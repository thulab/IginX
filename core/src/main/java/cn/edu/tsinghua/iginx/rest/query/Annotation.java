package cn.edu.tsinghua.iginx.rest.query;

import java.util.List;

public class Annotation
{
    List<String> tags;
    String text;
    String title;
    Long timestamp;
    Annotation(String str, Long tim)
    {
        timestamp = tim;
    }
}
