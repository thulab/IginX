package cn.edu.tsinghua.iginx.policy.naive;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Sampler {

    private final static Sampler instance = new Sampler();

    private final Set<String> prefixSet = new HashSet<>();

    private final List<String> prefixList = new LinkedList<>();

    private final int prefixMaxSize = 100;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Random random = new Random();

    private Sampler() {
    }

    public static Sampler getInstance() {
        return instance;
    }

    public void updatePrefix(List<String> paths) {
        lock.readLock().lock();
        if (prefixMaxSize <= prefixSet.size()) {
            lock.readLock().unlock();
            return;
        }
        lock.readLock().unlock();
        lock.writeLock().lock();
        if (prefixMaxSize <= prefixSet.size()) {
            lock.writeLock().unlock();
            return;
        }

        for (String path : paths) {
            if (path != null && !path.equals("")) {
                prefixSet.add(path);
                prefixList.add(path);
            }
        }
        lock.writeLock().unlock();
    }

    public List<String> samplePrefix(int count) {
        lock.readLock().lock();
        String[] prefixArray = new String[prefixList.size()];
        prefixList.toArray(prefixArray);
        lock.readLock().unlock();
        for (int i = 0; i < prefixList.size(); i++) {
            int next = random.nextInt(prefixList.size());
            String value = prefixArray[next];
            prefixArray[next] = prefixArray[i];
            prefixArray[i] = value;
        }
        List<String> resultList = new ArrayList<>();
        for (int i = 0; i < count && i < prefixArray.length; i++) {
            resultList.add(prefixArray[i]);
        }
        return resultList;
    }
}
