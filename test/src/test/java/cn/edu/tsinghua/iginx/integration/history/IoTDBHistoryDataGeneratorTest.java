package cn.edu.tsinghua.iginx.integration.history;

import org.apache.iotdb.session.Session;
import org.junit.Test;

public class IoTDBHistoryDataGeneratorTest {

    @Test
    public void writeHistoryDataToA() throws Exception {
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);");
        session.executeNonQueryStatement("INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71);");

        session.close();
    }

    @Test
    public void writeHistoryDataToB() throws Exception {
        Session session = new Session("127.0.0.1", 6668, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO root.ln.wf03.wt01(timestamp,status) values(77,true);");
        session.executeNonQueryStatement("INSERT INTO root.ln.wf03.wt01(timestamp,status,temperature) values(200,false,77.71);");

        session.close();
    }

}
