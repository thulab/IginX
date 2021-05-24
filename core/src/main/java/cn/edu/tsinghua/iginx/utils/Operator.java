package cn.edu.tsinghua.iginx.utils;

public class Operator {
	OperatorType operatorType;
	boolean reverse = false;

	Operator(OperatorType tp) {
		operatorType = tp;
	}

	public OperatorType getOperatorType() {
		return operatorType;
	}

	public void setOperatorType(OperatorType operatorType) {
		this.operatorType = operatorType;
	}

	void reverse() {
		if (operatorType == OperatorType.GT) operatorType = OperatorType.LTE;
		else if (operatorType == OperatorType.GTE) operatorType = OperatorType.LT;
		else if (operatorType == OperatorType.EQ) operatorType = OperatorType.NE;
		else if (operatorType == OperatorType.NE) operatorType = OperatorType.EQ;
		else if (operatorType == OperatorType.LTE) operatorType = OperatorType.GT;
		else if (operatorType == OperatorType.LT) operatorType = OperatorType.GTE;
		else if (operatorType == OperatorType.AND || operatorType == OperatorType.OR)
			reverse = true;
	}
}
