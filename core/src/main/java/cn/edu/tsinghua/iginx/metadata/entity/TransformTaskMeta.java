package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.thrift.UDFType;

public class TransformTaskMeta {

    private String name;

    private String className;

    private String fileName;

    private String ip;

    private UDFType type;

    public TransformTaskMeta(String name, String className, String fileName, String ip, UDFType type) {
        this.name = name;
        this.className = className;
        this.fileName = fileName;
        this.ip = ip;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public UDFType getType() {
        return type;
    }

    public void setType(UDFType type) {
        this.type = type;
    }

    public TransformTaskMeta copy() {
        return new TransformTaskMeta(name, className, fileName, ip, type);
    }

    @Override
    public String toString() {
        return "TransformTaskMeta{" +
            "name='" + name + '\'' +
            ", className='" + className + '\'' +
            ", fileName='" + fileName + '\'' +
            ", ip='" + ip + '\'' +
            ", type=" + type +
            '}';
    }
}
