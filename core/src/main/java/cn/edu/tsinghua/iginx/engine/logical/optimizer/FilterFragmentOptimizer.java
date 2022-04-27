package cn.edu.tsinghua.iginx.engine.logical.optimizer;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.logical.utils.OperatorUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterFragmentOptimizer implements Optimizer {

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(FilterFragmentOptimizer.class);

    private static FilterFragmentOptimizer instance;

    private FilterFragmentOptimizer() {

    }

    public static FilterFragmentOptimizer getInstance() {
        if (instance == null) {
            synchronized (FilterFragmentOptimizer.class) {
                if (instance == null) {
                    instance = new FilterFragmentOptimizer();
                }
            }
        }
        return instance;
    }

    @Override
    public Operator optimize(Operator root) {
        // only optimize query
        if (root.getType() == OperatorType.CombineNonQuery || root.getType() == OperatorType.ShowTimeSeries) {
            return root;
        }

        List<Select> selectOperatorList = new ArrayList<>();
        OperatorUtils.findSelectOperators(selectOperatorList, root);

        if (selectOperatorList.isEmpty()) {
            logger.info("There is no filter in logical tree.");
            return root;
        }

        for (Select selectOperator : selectOperatorList) {
            filterFragmentByTimeRange(selectOperator);
        }
        return root;
    }

    private void filterFragmentByTimeRange(Select selectOperator) {
        List<String> pathList = OperatorUtils.findPathList(selectOperator);
        if (pathList.isEmpty()) {
            logger.error("Can not find paths in select operator.");
            return;
        }

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));
        Map<TimeSeriesInterval, List<FragmentMeta>> fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);

        Filter filter = selectOperator.getFilter();
        List<TimeRange> timeRanges = ExprUtils.getTimeRangesFromFilter(filter);

        List<Operator> joinList = new ArrayList<>();
        fragments.forEach((k, v) -> {
            List<Operator> unionList = new ArrayList<>();
            v.forEach(meta -> {
                if (hasTimeRangeOverlap(meta, timeRanges)) {
                    unionList.add(new Project(new FragmentSource(meta), pathList));
                }
            });
            Operator operator = OperatorUtils.unionOperators(unionList);
            if (operator != null) {
                joinList.add(operator);
            }
        });

        Operator root = OperatorUtils.joinOperatorsByTime(joinList);
        if (root != null) {
            selectOperator.setSource(new OperatorSource(root));
        }
    }

    private boolean hasTimeRangeOverlap(FragmentMeta meta, List<TimeRange> timeRanges) {
        TimeInterval interval = meta.getTimeInterval();
        for (TimeRange range : timeRanges) {
            if (interval.getStartTime() > range.getEndTime() ||
                interval.getEndTime() < range.getBeginTime()) {
                // continue
            } else {
                return true;
            }
        }
        return false;
    }
}
