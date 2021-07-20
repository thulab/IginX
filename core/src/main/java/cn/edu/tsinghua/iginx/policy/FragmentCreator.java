package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.utils.Pair;
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
    private static Timer timer2 = new Timer();
    private static final Logger LOGGER = LoggerFactory.getLogger(FragmentCreator.class);
    private Map<String, Double> prefixList = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final IMetaManager iMetaManager = DefaultMetaManager.getInstance();
    private final IPolicy iPolicy = PolicyManager.getInstance().getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());
    private final Random random = new Random();
    private final int prefixMaxSize = 1048576;
    private int updateRequireNum = 0;
    private static int LIMIT = -1;
    private static int ts = -1;
    final Semaphore semp = new Semaphore(0);
    final Semaphore semp2 = new Semaphore(0);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static FragmentCreator INSTANCE = null;
    private int fragmentNum;
    private long fragmentTime;
    private long updateRequire = 0;

    private FragmentCreator()
    {
        init(config.getReallocateTime());
        fragmentNum = config.getFragmentSplitPerEngine() * iMetaManager.getStorageEngineNum();
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

    public void tryGet()
    {
        LOGGER.info("insert tryGet");
        lock.writeLock().lock();
        if (LIMIT == -1)
            LIMIT = ts * iMetaManager.getIginxList().size();
        try
        {
            prefixList = iMetaManager.getPrefix();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        if (prefixList.size() >= LIMIT)
        {
            semp.release();
            LIMIT += ts * iMetaManager.getIginxList().size();
            LOGGER.info("semp1 release");
        }
        LOGGER.info("tryGet end, list size : {}", prefixList.size());
        lock.writeLock().unlock();
    }


    public void setFragmentData(int fragment, long timestamp)
    {
        LOGGER.info("insert setFragmentData");
        fragmentTime = timestamp;
        fragmentNum = fragment;

        lock.writeLock().lock();
        updateRequire += 1;
        if (updateRequire >= iMetaManager.getIginxList().size())
        {
            updateRequire -= iMetaManager.getIginxList().size();
            semp2.release();
            LOGGER.info("semp2 release");
        }
        LOGGER.info("setFragmentData end, fragmentTime : {}, fragmentNum : {}", fragmentTime, fragmentNum);
        lock.writeLock().unlock();
    }



    public void CreateFragment() throws Exception
    {
        LOGGER.info("insert CreateFragment");
        LOGGER.info("create fragment  , list size : {}", prefixList.size());
        if (((DefaultMetaManager)iMetaManager).storage.selection())
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

            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = iPolicy.getIFragmentGenerator().generateFragmentsAndStorageUnits(samplePrefix(fragmentNum - 1), fragmentTime);
            iMetaManager.createFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            LOGGER.info("create fragment  , size : {}", prefixList.size());
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

        timer2.schedule(new TimerTask()
        {
            @Override
            public void run()
            {
                try
                {
                    tryGet();
                }
                catch (Exception e)
                {
                    LOGGER.error("Error occurs when create fragment : {}", e);
                    e.printStackTrace();
                }
            }
        }, length / 5, length / 10);
    }

}


