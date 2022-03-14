package cn.edu.tsinghua.iginx.session;

import java.util.Collections;

/**
 * Created on 09/03/2022.
 * Description:
 *
 * @author ziyuan
 */
public class ExampleTest {

    public static void main(String[] args) throws Exception {
        Session session = new Session("127.0.0.1", 6888, "root", "root");
        session.openSession();

        SessionQueryDataSet sessionQueryDataSet = session.queryData(Collections.singletonList("a.b"), 10, 12);
        sessionQueryDataSet.print();

        // 关闭 session
        session.closeSession();
    }

}
