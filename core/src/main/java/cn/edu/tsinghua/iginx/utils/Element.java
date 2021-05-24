package cn.edu.tsinghua.iginx.utils;

public class Element {
	Type type;
	Operator operator;
	String value;

	public Type getType() {
		return type;
	}

	public Operator getOperator() {
		return operator;
	}

	public String getValue() {
		return value;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public void setValue(String value) {
		this.value = value;
	}
}
