package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class FragmentCreator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCreator.class);
    private final Set<String> prefixSet = new HashSet<>();
    private final List<String> prefixList = new LinkedList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final IMetaManager iMetaManager = SortedListAbstractMetaManager.getInstance();
    private final Random random = new Random();
    private final int prefixMaxSize = 1048576;
    private int updateRequireNum = 0;

    private static FragmentCreator INSTANCE = null;

    public static FragmentCreator getInstance() {
        if (INSTANCE == null) {
            synchronized (FragmentCreator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FragmentCreator();
                }
            }
        }
        return INSTANCE;
    }

    public void updatePrefix(List<String> ins) {
        lock.writeLock().lock();
        for (String prefix : ins)
        {
            if (!prefixSet.contains(prefix))
            {
                if (prefixSet.size() == prefixMaxSize)
                {
                    int tmp = random.nextInt(prefixMaxSize);
                    prefixSet.remove(prefixList.get(tmp));
                    prefixList.remove(tmp);
                }
                prefixSet.add(prefix);
                prefixList.add(prefix);
            }
        }
        lock.writeLock().unlock();
    }

    public List<FragmentMeta> generateFragments(List<String> prefixList, long startTime) {
        List<FragmentMeta> resultList = new ArrayList<>();
        prefixList = prefixList.stream().filter(Objects::nonNull).sorted(String::compareTo).collect(Collectors.toList());
        String previousPrefix;
        String prefix = null;
        int from = 0;
        for (String s : prefixList) {
            previousPrefix = prefix;
            prefix = s;
            resultList.add(new FragmentMeta(previousPrefix, prefix, startTime, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment(from)));
            from++;
        }
        resultList.add(new FragmentMeta(prefix, null, startTime, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment(from)));
        return resultList;
    }

    public List<Long> chooseStorageEngineIdListForNewFragment(int from) {
        List<Long> storageEngineIdList = iMetaManager.getStorageEngineList().stream().map(StorageEngineMeta::getId).collect(Collectors.toList());
        if (storageEngineIdList.size() <= 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum()) {
            return storageEngineIdList;
        }
        Collections.sort(storageEngineIdList);
        for (int i = 0; i < from; i++) {
            Long next = storageEngineIdList.get(0);
            storageEngineIdList.remove(0);
            storageEngineIdList.add(next);
        }
        return storageEngineIdList.subList(0, 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum());
    }

    public void CreateFragment(int fragmentNum, int timestamp)
    {
        lock.writeLock().lock();
        updateRequireNum += 1;
        LOGGER.info("create fragment  , list size : {}", prefixList.size());
        if (updateRequireNum == 4)
        {
            List<FragmentMeta> fragments = generateFragments(samplePrefix(fragmentNum - 1), timestamp);
            LOGGER.info("create fragment  , size : {}", prefixList.size());
            iMetaManager.createFragments(fragments);
            updateRequireNum = 0;
        }
        lock.writeLock().unlock();
    }

    public List<String> samplePrefix(int count) {
        String[] prefixArray = new String[prefixList.size()];
        prefixList.toArray(prefixArray);
        Collections.sort(prefixList, String::compareTo);
        List<String> resultList = new ArrayList<>();
        if (prefixArray.length <= count) {
            for (int i = 0; i < prefixArray.length; i++) {
                resultList.add(prefixArray[i]);
            }
        } else {
            for (int i = 0; i < count; i++) {
                int tmp = prefixArray.length * (i + 1) / (count + 1);
                resultList.add(prefixArray[tmp]);
            }
        }
        return resultList;
    }
}
