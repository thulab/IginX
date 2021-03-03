package cn.edu.tsinghua.iginx.fragment;

import cn.edu.tsinghua.iginx.metadata.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.MetaManager;

public class FragmentProcessor {

	// 拆分计划时调用
	public static void createFragment(String key, long startTime, long endTime) {
		long databaseId = MetaManager.getInstance().chooseDatabaseForNewFragment().getId();
		MetaManager.getInstance().createFragment(new FragmentMeta(key, startTime, endTime, databaseId));
	}
}
