package cn.edu.tsinghua.iginx.rest.bean;

import lombok.Data;
import java.util.*;

@Data
public class AnnotationLimit {
    private List<String> tag = new ArrayList<>();//LHZ应当改为set集合
    private String text = ".*";
    private String title = ".*";

    public void addTag(String key) {
        tag.add(key);
    }
}
