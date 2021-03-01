package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exceptions.StatementExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;

public class RpcUtils {

	public static void verifySuccess(Status status) throws StatementExecutionException {
		if (status.code != StatusCode.SUCCESS_STATUS.getStatusCode()) {
			throw new StatementExecutionException(status);
		}
	}
}
