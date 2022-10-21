package cn.edu.tsinghua.iginx.integration.history;

import org.apache.iotdb.session.Session;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBHistoryDataGenerator.class);

    @Test
    public void OriHasDataExpHasData() throws Exception {
        writeHistoryDataToA();
        writeHistoryDataToB();
    }

    @Test
    public void OriHasDataExpNoData() throws Exception {
        writeHistoryDataToA();
    }

    @Test
    public void OriNoDataExpHasData() throws Exception {
        writeHistoryDataToB();
    }

    @Test
    public void OriNoDataExpNoData() throws Exception {
    }

    @Test
    public void clearData() {
        try {
            Session sessionA = new Session("127.0.0.1", 6667, "root", "root");
            sessionA.open();
            sessionA.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionA.close();

            Session sessionB = new Session("127.0.0.1", 6668, "root", "root");
            sessionB.open();
            sessionB.executeNonQueryStatement("DELETE STORAGE GROUP root.*");
            sessionB.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        logger.info("clear data success!");
    }

    public void writeHistoryDataToA() throws Exception {
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true);");
        session.executeNonQueryStatement("INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71);");

        session.close();

        logger.info("write data to 127.0.0.1:6667 success!");
    }

    public void writeHistoryDataToB() throws Exception {
        Session session = new Session("127.0.0.1", 6668, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO root.ln.wf03.wt01(timestamp,status) values(77,true);");
        session.executeNonQueryStatement("INSERT INTO root.ln.wf03.wt01(timestamp,status,temperature) values(200,false,77.71);");

        session.close();

        logger.info("write data to 127.0.0.1:6668 success!");
    }

}
