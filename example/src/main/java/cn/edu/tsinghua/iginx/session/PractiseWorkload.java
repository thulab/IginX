package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PractiseWorkload {

    private static Session session;

    private static final String template = "cpu.usage.h%03d.c%03d";

    private static final int hostCount = 200;

    private static final int corePerHost = 5;

    private static final int size = 10;

    private static final long startTime = System.currentTimeMillis();

    private static final double prob = 0.80;

    private static final Random random = new Random(0);

    private static final int totalBatch = 10000;

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6888, "root", "root");
        session.openSession();
        writePoints();
        session.closeSession();
    }

    private static void writePoints() throws Exception {
        int batchCount = 0;

        long offset = 0;
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < hostCount; i++) {
            for (int j = 0; j < corePerHost; j++) {
                paths.add(String.format(template, i, j));
                dataTypeList.add(DataType.INTEGER);
            }
        }

        while (batchCount < totalBatch) {
            Object[] valuesList = new Object[size];
            long[] timestamps = new long[size];
            for (int i = 0; i < size; i++) {
                Object[] values = new Object[paths.size()];
                for (int j = 0; j < values.length; j++) {
                    if (random.nextDouble() < prob) {
                        values[j] = random.nextInt(100);
                    } else {
                        values[j] = null;
                    }
                }
                valuesList[i] = values;
                timestamps[i] = startTime + (offset++) * 1000;
            }
            session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
            batchCount += 1;
        }



    }

}
