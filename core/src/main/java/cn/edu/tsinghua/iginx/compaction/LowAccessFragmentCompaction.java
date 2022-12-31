package cn.edu.tsinghua.iginx.compaction;

public class LowAccessFragmentCompaction implements ICompaction {
    @Override
    public boolean needCompaction() {
        return false;
    }

    @Override
    public void merge() {

    }
}
