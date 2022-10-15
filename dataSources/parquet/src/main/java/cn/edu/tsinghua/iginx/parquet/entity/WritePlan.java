package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import java.nio.file.Path;
import java.util.List;

public class WritePlan {

    private Path filePath;

    private List<String> pathList;

    private TimeInterval timeInterval;

    public WritePlan(Path filePath, List<String> pathList, TimeInterval timeInterval) {
        this.filePath = filePath;
        this.pathList = pathList;
        this.timeInterval = timeInterval;
    }

    public Path getFilePath() {
        return filePath;
    }

    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }

    public List<String> getPathList() {
        return pathList;
    }

    public void setPathList(List<String> pathList) {
        this.pathList = pathList;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(TimeInterval timeInterval) {
        this.timeInterval = timeInterval;
    }
}
