package cn.edu.tsinghua.iginx.parquet.exec;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;

public interface Executor {

    TaskExecuteResult executeProjectTask(List<String> paths, TagFilter tagFilter, String filter,
        String storageUnit, boolean isDummyStorageUnit);

    TaskExecuteResult executeInsertTask(DataView dataView, String storageUnit);

    TaskExecuteResult executeDeleteTask(List<String> paths, List<TimeRange> timeRanges,
        TagFilter tagFilter, String storageUnit);

    List<Timeseries> getTimeSeriesOfStorageUnit(String storageUnit) throws PhysicalException;

    Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage() throws PhysicalException;

    void close() throws PhysicalException;

}
