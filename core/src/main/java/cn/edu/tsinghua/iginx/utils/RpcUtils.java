package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;

public class RpcUtils {

	public static void verifySuccess(Status status) throws ExecutionException {
		if (status.code != StatusCode.SUCCESS_STATUS.getStatusCode()) {
			throw new ExecutionException(status);
		}
	}
}
