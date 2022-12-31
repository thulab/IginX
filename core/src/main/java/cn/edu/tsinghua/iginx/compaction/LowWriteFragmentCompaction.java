package cn.edu.tsinghua.iginx.compaction;

public class LowWriteFragmentCompaction implements ICompaction {
    @Override
    public boolean needCompaction() {
        return false;
    }

    @Override
    public void merge() {

    }
}
