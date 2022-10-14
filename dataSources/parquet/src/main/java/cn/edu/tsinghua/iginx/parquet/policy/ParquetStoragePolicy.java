package cn.edu.tsinghua.iginx.parquet.policy;

import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;

public interface ParquetStoragePolicy {

    Pair<Long, List<String>> getLatestPartition(String storageUnit);

    TreeMap<Long, List<String>> getAllPartition(String storageUnit);

    FlushType getFlushType(Path outputFilePath, long pointsNum);

    enum FlushType {
        ORIGIN,
        APPENDIX
    }
}
