package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class IginXStreamSessionExample {

    public static void main(String[] args) throws Exception {
        Session session = new Session("127.0.0.1", 6888, "root", "root");

        session.openSession();

        QueryDataSet dataSet = session.executeQuery("select * from computing_center", 10);

        List<String> columns = dataSet.getColumnList();
        List<DataType> dataTypes = dataSet.getDataTypeList();

        for (String column: columns) {
            System.out.print(column + "\t");
        }
        System.out.println();

        while (dataSet.hasMore()) {
            Object[] row = dataSet.nextRow();
            for (Object o: row) {
                System.out.print(o);
                System.out.print("\t\t\t");
            }
            System.out.println();
        }
        dataSet.close();

        session.closeSession();
    }

}
