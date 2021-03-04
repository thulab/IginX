package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.Iginx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentUtils {

	private static final Logger logger = LoggerFactory.getLogger(EnvironmentUtils.class);

	private static Iginx iginx;

	public static void setUpEnv() {
		iginx = new Iginx();
	}

	public static void tearDownEnv() {

	}
}
