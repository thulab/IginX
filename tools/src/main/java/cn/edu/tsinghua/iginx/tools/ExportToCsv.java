package cn.edu.tsinghua.iginx.tools;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;

public class ExportToCsv {
    private static Map<Long, Map<String, Object>> dataTable = new LinkedHashMap<>();
    private static final Config CONFIG = ConfigDescriptor.getInstance().getConfig();
    public static String MACHINE_LIST = CONFIG.getMachineList();
    public static String METRIC_LIST = CONFIG.getMetricList();
    public static String START_TIME = CONFIG.getStartTime();
    public static String ENDED_TIME = CONFIG.getEndTime();
    public static String EXPORT_FILE_DIR = CONFIG.getExportFileDir();
    public static String ip = CONFIG.getIp();
    public static Integer port = CONFIG.getPort();
    public static String user = CONFIG.getUsername();
    public static String password = CONFIG.getPassword();
    private static String dirAbsolutePath;


    public static void main(String[] args) {
        String[] metrics = METRIC_LIST.split(",");
        String[] machines = MACHINE_LIST.split(",");
        dirAbsolutePath = EXPORT_FILE_DIR + File.separator + String.format(Constants.DIR_NAME, MACHINE_LIST, START_TIME, ENDED_TIME);
        if (!new File(dirAbsolutePath).exists()) {
            new File(dirAbsolutePath).mkdir();
        }


        if (!"".equals(START_TIME) && !"".equals(ENDED_TIME)) {
            long startTime = convertDateStrToTimestamp(START_TIME);
            long endTime = convertDateStrToTimestamp(ENDED_TIME);
            if (startTime == -1L || endTime == -1L) {
                return;
            }
            System.out.println("开始查询Iginx的数据...");
            long start;
            long loadElapse = 0;
            long exportCsvElapse = 0;
            for (long i = startTime; i < endTime; i += Constants.MILLS_PER_DAY) {
                for (String machine : machines) {
                    start = System.currentTimeMillis();
                    loadAllMetricsOfOneTrainOneDay(machine, metrics, i, Math.min(endTime, i + Constants.MILLS_PER_DAY));
                    loadElapse += System.currentTimeMillis() - start;

                    start = System.currentTimeMillis();
                    exportDataTable(machine, metrics, i);
                    exportCsvElapse += System.currentTimeMillis() - start;
                }
            }
            System.out.println("查询Iginx的数据耗时 " + loadElapse + " ms, " + "导出成CSV文件耗时 " + exportCsvElapse + " ms");
        } else {
            System.out.println("必须指定导出数据的起止时间！");
        }


    }


    private static void loadAllMetricsOfOneTrainOneDay(String machine, String[] metrics, long startTime, long endTime) {
        Session session = new Session(ip, port, user, password);
        try {
            session.openSession();
        } catch (SessionException e) {
            e.printStackTrace();
        }
        try {
            List<String> paths = new ArrayList<>();
            for (int i = 0; i < Constants.PATH.length; i++) {
                for (int j = 0; j < metrics.length; j++) {
                    paths.add(String.format(Constants.PATH[i], machine, metrics[j]));
                }
            }
            for (String path : paths) {
                System.out.println(path);
            }
            SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);

            for (int i = 0; i < dataSet.getTimestamps().length; i++) {
                long timestamp =  dataSet.getTimestamps()[i];
                Map<String, Object> ins = new HashMap<>();
                for (int j = 0; j < dataSet.getPaths().size(); j++) {
                    String metric = dataSet.getPaths().get(j).split("\\.")[dataSet.getPaths().get(j).split("\\.").length - 1];
                    if (dataSet.getValues().get(i).get(j) instanceof byte[]) {
                        ins.put(metric, new String((byte[]) dataSet.getValues().get(i).get(j)));
                    } else {
                        ins.put(metric, dataSet.getValues().get(i).get(j));
                    }
                }
                dataTable.put(timestamp, ins);
            }
            System.out.println("Printing ResultSets Finished.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                session.closeSession();
            } catch (SessionException e) {
                e.printStackTrace();
            }
        }
    }

    private static void exportDataTable(String machine, String[] metrics, long startTime) {
        String machinePath = dirAbsolutePath + File.separator + machine;
        if (!new File(machinePath).exists()) {
            new File(machinePath).mkdir();
        }
        String csvFileName = machinePath + File.separator  + String.format(Constants.CSV_FILE_NAME, machine, startTime);
        File file = new File(csvFileName);
        System.out.printf("正在导出%d列, %d行数据到 %s ...%n", metrics.length, dataTable.size(), csvFileName);

        int count = 0;
        int stage = dataTable.size() / 20;
        try {
            try (FileWriter writer = new FileWriter(file)) {
                StringBuilder headBuilder = new StringBuilder();
                headBuilder.append("Time");
                for (String metric : metrics) {
                    headBuilder.append(",").append(metric);
                }
                headBuilder.append("\n");
                writer.write(headBuilder.toString());
                for (Map.Entry<Long, Map<String, Object>> entry : dataTable.entrySet()) {
                    StringBuilder lineBuilder = new StringBuilder(entry.getKey() + "");
                    Map<String, Object> record = entry.getValue();
                    for (String metric : metrics) {
                        Object value = record.get(metric);
                        if (value != null) {
                            lineBuilder.append(",").append(value);
                        } else {
                            lineBuilder.append(",");
                        }
                    }
                    lineBuilder.append("\n");
                    writer.write(lineBuilder.toString());
                    count++;
                    if (stage > 0) {
                        if (count % stage == 0) {
                            double a = count * 100.0 / dataTable.size();
                            String p = String.format("%.1f", a);
                            System.out.println("已完成 " + p + "%");
                        }
                    } else {
                        double a = count * 100.0 / dataTable.size();
                        String p = String.format("%.1f", a);
                        System.out.println("已完成 " + p + "%");
                    }
                }
            }
            dataTable = new LinkedHashMap<>();
        } catch (IOException e) {
            System.out.println("Iginx数据导出为CSV文件失败");
            e.printStackTrace();
        }
    }


    public static Long convertDateStrToTimestamp(String s){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.CHINA);
        Date date;
        try {
            date = simpleDateFormat.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
            return -1L;
        }
        return date.getTime();
    }
}
