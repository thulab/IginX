package cn.edu.tsinghua.iginx.compaction;

public interface ICompaction {
    boolean needCompaction();

    void merge();
}