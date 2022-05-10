package cn.edu.tsinghua.iginx.metadata.entity;

public class TransformTaskMeta {

    private String className;

    private String fileName;

    private String ip;

    public TransformTaskMeta(String className, String fileName, String ip) {
        this.className = className;
        this.fileName = fileName;
        this.ip = ip;
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

    public TransformTaskMeta copy() {
        return new TransformTaskMeta(className, fileName, ip);
    }

    @Override
    public String toString() {
        return "TransformTaskMeta{" +
            "className='" + className + '\'' +
            ", fileName='" + fileName + '\'' +
            ", ip='" + ip + '\'' +
            '}';
    }
}
