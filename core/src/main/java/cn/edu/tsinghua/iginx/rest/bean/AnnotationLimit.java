package cn.edu.tsinghua.iginx.rest.bean;

import lombok.Data;

@Data
public class AnnotationLimit {
    private String tag = ".*";
    private String text = ".*";
    private String title = ".*";
}
