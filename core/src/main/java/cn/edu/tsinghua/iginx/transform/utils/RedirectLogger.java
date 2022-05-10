package cn.edu.tsinghua.iginx.transform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Scanner;

public class RedirectLogger extends Thread {

    private final InputStream inputStream;

    private final String name;

    private final static Logger logger = LoggerFactory.getLogger(RedirectLogger.class);

    public RedirectLogger(InputStream inputStream, String name) {
        this.inputStream = inputStream;
        this.name = name;
    }

    @Override
    public void run() {
        logger.info("hello");
//        Scanner scanner = new Scanner(inputStream);
//        while (scanner.hasNextLine()) {
//            logger.info(String.format("[Python %s] ", name) + scanner.nextLine());
//        }
        try {
            byte[] buffer = new byte[1024];
            int len = -1;
            while((len = inputStream.read(buffer)) > 0){
                System.out.write(buffer, 0, len);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
