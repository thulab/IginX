package cn.edu.tsinghua.iginx.integration.history;

import org.apache.iotdb.session.Session;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBHistoryDataGenerator {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBHistoryDataGenerator.class);

    protected boolean oriHasData = false;
    protected boolean expansionHasData = false;

    public void init() throws Exception {
        if(oriHasData)
            writeHistoryDataToA();
        if(expansionHasData)
            writeHistoryDataToB();
    }

    public void writeHistoryDataToA() throws Exception {
        Session session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO ln.wf01.wt01(timestamp,status) values(100,true);");
        session.executeNonQueryStatement("INSERT INTO ln.wf01.wt01(timestamp,status,temperature) values(200,false,20.71);");

        session.close();

        logger.info("write data to 127.0.0.1:6667 success!");
    }


    public void writeHistoryDataToB() throws Exception {
        Session session = new Session("127.0.0.1", 6668, "root", "root");
        session.open();

        session.executeNonQueryStatement("INSERT INTO ln.wf03.wt01(timestamp,status) values(77,true);");
        session.executeNonQueryStatement("INSERT INTO ln.wf03.wt01(timestamp,status,temperature) values(200,false,77.71);");

        session.close();

        logger.info("write data to 127.0.0.1:6668 success!");
    }

}