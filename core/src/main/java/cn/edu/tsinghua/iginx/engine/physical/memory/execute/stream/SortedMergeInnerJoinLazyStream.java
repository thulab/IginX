package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * input two stream must be ascending order.
 * */
public class SortedMergeInnerJoinLazyStream extends BinaryLazyStream {

    private final InnerJoin innerJoin;

    private Header header;

    private boolean hasInitialized = false;

    private String joinColumnA;

    private String joinColumnB;

    private Row nextA;

    private Row nextB;

    private int index;

    private Value curJoinColumnBValue;  // 当前StreamB中join列的值，用于同值join

    private final List<Row> sameValueStreamBRows;  // StreamB中join列的值相同的列缓存

    private final Deque<Row> cache;

    public SortedMergeInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.innerJoin = innerJoin;
        this.sameValueStreamBRows = new ArrayList<>();
        this.cache = new LinkedList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = innerJoin.getFilter();

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

        List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
        if (innerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, headerA, headerB,
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException("sorted merge join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("sorted merge join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                this.joinColumnA = p.k.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.v.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                this.joinColumnA = p.v.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.k.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("sorted merge join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                this.joinColumnA = this.joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join column input.");
            }
        }
        this.index = headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumnB);

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            this.header = RowUtils.constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(),
                Collections.singletonList(joinColumnB), true).getV();
        }

        this.hasInitialized = true;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        while (cache.isEmpty() && hasMoreRows()) {
            tryMatch();
        }
        return !cache.isEmpty();
    }

    private void tryMatch() throws PhysicalException {
        Value curJoinColumnAValue = nextA.getAsValue(innerJoin.getPrefixA() + "." + joinColumnA);
        int cmp = ValueUtils.compare(curJoinColumnAValue, curJoinColumnBValue);
        if (cmp < 0) {
            nextA = null;
        } else if (cmp > 0) {
            sameValueStreamBRows.clear();
        } else {
            for (Row rowB : sameValueStreamBRows) {
                if (innerJoin.getFilter() != null) {
                    Row row = RowUtils.constructNewRow(header, nextA, rowB);
                    if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row = RowUtils.constructNewRow(header, nextA, rowB, new int[]{index}, true);
                    cache.addLast(row);
                }
            }
            nextA = null;
        }
    }

    private boolean hasMoreRows() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        while (nextA == null && streamA.hasNext()) {
            nextA = streamA.next();
        }
        while (nextB == null && streamB.hasNext()) {
            nextB = streamB.next();
        }
        while (sameValueStreamBRows.isEmpty() && nextB != null) {
            sameValueStreamBRows.add(nextB);
            curJoinColumnBValue = nextB.getAsValue(innerJoin.getPrefixB() + "." + joinColumnB);
            nextB = null;

            while (streamB.hasNext()) {
                nextB = streamB.next();
                Value joinColumnBValue = nextB.getAsValue(innerJoin.getPrefixB() + "." + joinColumnB);
                if (ValueUtils.compare(joinColumnBValue, curJoinColumnBValue) == 0) {
                    sameValueStreamBRows.add(nextB);
                    nextB = null;
                } else {
                    break;
                }
            }
        }
        return nextA != null && !sameValueStreamBRows.isEmpty();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }
}
