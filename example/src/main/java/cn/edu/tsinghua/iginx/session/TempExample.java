package cn.edu.tsinghua.iginx.session;

import java.util.List;

/**
 * Created on 10/01/2022.
 * Description:
 *
 * @author ziyuan
 */
public class TempExample {

    private static Session session;

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6888, "root", "root");


        session.openSession();

        List<String> paths = session.showChildPaths("a.b.a");

        for (String path: paths) {
            System.out.println(path);
        }

        session.closeSession();
    }

}
