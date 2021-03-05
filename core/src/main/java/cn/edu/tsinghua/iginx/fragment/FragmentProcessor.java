package cn.edu.tsinghua.iginx.fragment;

import cn.edu.tsinghua.iginx.metadata.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.MetaManager;

import java.util.List;

public class FragmentProcessor {

	// 拆分计划时调用
	public static FragmentMeta createFragment(String key, long startTime, long endTime) {
		List<Long> databaseIds = MetaManager.getInstance().chooseDatabaseIdsForNewFragment();
		FragmentMeta fragment = new FragmentMeta(key, startTime, endTime, databaseIds);
		MetaManager.getInstance().createFragment(fragment);
		return fragment;
	}
}
