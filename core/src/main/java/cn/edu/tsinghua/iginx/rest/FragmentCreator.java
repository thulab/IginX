package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class FragmentCreator
{
    private static Timer timer = new Timer();
    private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCreator.class);
    private final Map<String, Double> prefixList = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final IMetaManager iMetaManager = SortedListAbstractMetaManager.getInstance();
    private final Random random = new Random();
    private final int prefixMaxSize = 1048576;
    private int updateRequireNum = 0;
    private static int LIMIT = -1;
    private static int ts = -1;
    private static int cntRequire = 0;
    final Semaphore semp = new Semaphore(0);
    final Semaphore semp2 = new Semaphore(0);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static FragmentCreator INSTANCE = null;
    private int fragmentNum;
    private long fragmentTime;

    private FragmentCreator()
    {
        init(config.getReallocateTime());
        fragmentNum = config.getFragmentSplitPerEngine() * iMetaManager.getIginxList().size();
        fragmentTime = System.currentTimeMillis();
        ts = config.getPathSendSize();
    }

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
        LOGGER.info("insert updatePrefix");
        lock.writeLock().lock();
        if (LIMIT == -1)
            LIMIT = ts * iMetaManager.getIginxList().size();
        LOGGER.info("update prefix  , list size : {}", prefixList.size());
        for (String prefix : ins)
        {
            String[] tmp = prefix.split("\2");
            double value = Double.parseDouble(tmp[1]);
            if (prefixList.containsKey(tmp[0]))
            {
                value += prefixList.get(tmp[0]);
            }
            prefixList.put(tmp[0], value);
        }
        if (prefixList.size() >= LIMIT)
        {
            semp.release();
            LIMIT += ts * iMetaManager.getIginxList().size();
            LOGGER.info("semp release");
        }
        LOGGER.info("update prefix  end, list size : {}", prefixList.size());
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

    public void setFragmentData(int fragment, long timestamp)
    {
        fragmentTime = timestamp;
        fragmentNum = fragment;

        lock.writeLock().lock();
        cntRequire += 1;
        if (cntRequire >= 1)
        {
            semp2.release();
            cntRequire = 0;
        }
        lock.writeLock().unlock();
    }

    public void CreateFragment() throws Exception
    {
        LOGGER.info("insert CreateFragment");
        LOGGER.info("create fragment  , list size : {}", prefixList.size());
        if (iMetaManager.selection())
        {
            try
            {
                semp.acquire();
                semp2.acquire();
                LOGGER.info("semp acquire");
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            List<FragmentMeta> fragments = generateFragments(samplePrefix(fragmentNum - 1), fragmentTime);
            LOGGER.info("create fragment  , size : {}", prefixList.size());
            iMetaManager.createFragments(fragments);
            updateRequireNum = 0;
        }
    }

    public List<String> samplePrefix(int count) {
        String[] prefixArray = prefixList.keySet().toArray(new String[prefixList.size()]);
        Arrays.sort(prefixArray, String::compareTo);
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

    public void init(int length)
    {
        timer.schedule(new TimerTask()
        {
                @Override
                public void run()
                {
                    try
                    {
                        CreateFragment();
                    }
                    catch (Exception e)
                    {
                        LOGGER.error("Error occurs when create fragment : {}", e);
                        e.printStackTrace();
                    }
                }
        }, 0, length);
    }
}


