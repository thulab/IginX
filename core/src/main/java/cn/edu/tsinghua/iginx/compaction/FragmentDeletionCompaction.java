package cn.edu.tsinghua.iginx.compaction;

public class FragmentDeletionCompaction implements ICompaction {
    @Override
    public boolean needCompaction() {
        return false;
    }

    @Override
    public void merge() {

    }
}
