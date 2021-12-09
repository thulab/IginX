package cn.edu.tsinghua.iginx.session;


import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.write.Point;

public class NewSessionExample {

    public static void main(String[] args) {
        IginXClient client = IginXClientFactory.create();
        WriteClient writeClient = client.getWriteClient();
        writeClient.writePoint(
                Point.builder()
                        .now()
                        .measurement("a.a.a")
                        .intValue(2333)
                        .build()
        );
        client.close();
    }

}
