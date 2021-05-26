/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.query.expression;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class BooleanExpression {
	private String boolExpression;
	private List<Element> postfixExpression;
	private TreeNode root;
	private List<String> timeseries = new ArrayList<>();

	public BooleanExpression(String str) {
		boolExpression = str;
		postfixExpression = analysis(str);
		root = build(postfixExpression);
		if (root != null) {
			dfs(root);
		}
	}

	public boolean getBool(Map<String, Object> values) {
		return getAnswer(root, values);
	}

	private boolean getAnswer(TreeNode now, Map<String, Object> values) {
		if (now.getData().getType() == Type.OPERATOR && getOperatorPriority(now.getData().getOperator().getOperatorType()) == 0) {
			try {
				Object value = values.get(now.getLeft().getData().getTimeseries());
				if (value == null)
					return false;
				String compareValue = now.getRight().getData().getValue();
				if (value instanceof Boolean) {
					return isBool((boolean) value, Boolean.parseBoolean(compareValue), now.getData().getOperator());
				} else if (value instanceof Integer) {
					return isBool((int) value, Integer.parseInt(compareValue), now.getData().getOperator());
				} else if (value instanceof Long) {
					return isBool((long) value, Long.parseLong(compareValue), now.getData().getOperator());
				} else if (value instanceof Float) {
					return isBool((float) value, Float.parseFloat(compareValue), now.getData().getOperator());
				} else if (value instanceof Double) {
					return isBool((double) value, Double.parseDouble(compareValue), now.getData().getOperator());
				} else if (value instanceof byte[]) {
					return isBool(new String((byte[]) value), compareValue, now.getData().getOperator());
				} else {
					return false;
				}
			} catch (Exception e) {
				return false;
			}
		} else {
			Boolean leftAns = getAnswer(now.getLeft(), values);
			Boolean rightAns = getAnswer(now.getLeft(), values);
			if (now.getData().getOperator().getOperatorType() == OperatorType.AND)
				return leftAns && rightAns;
			else
				return leftAns || rightAns;
		}
	}

	private <T extends Comparable<T>> boolean isBool(T left, T right, Operator op) {
		switch (op.getOperatorType()) {
			case GT:
				return left.compareTo(right) > 0;
			case GTE:
				return left.compareTo(right) >= 0;
			case EQ:
				return left == right;
			case NE:
				return left != right;
			case LTE:
				return left.compareTo(right) <= 0;
			case LT:
				return left.compareTo(right) < 0;
			default:
				return false;
		}
	}

	private void dfs(TreeNode now) {
		if (now.getData().getType() == Type.OPERATOR && now.getData().getOperator().isReversed()) {
			now.getData().getOperator().setReversed(false);
			if (now.getData().getOperator().getOperatorType() == OperatorType.AND ||
					now.getData().getOperator().getOperatorType() == OperatorType.OR) {
				now.getLeft().getData().getOperator().reverse();
				now.getRight().getData().getOperator().reverse();
				if (now.getData().getOperator().getOperatorType() == OperatorType.AND)
					now.getData().setOperator(new Operator(OperatorType.OR));
				else
					now.getData().setOperator(new Operator(OperatorType.AND));
			} else
				now.getData().getOperator().reverse();
		}
		if (now.getData().getType() == Type.TIMESERIES)
			timeseries.add(now.getData().getTimeseries());
		if (now.getLeft() != null)
			dfs(now.getLeft());
		if (now.getRight() != null)
			dfs(now.getRight());
	}


	public int getOperatorPriority(OperatorType tp) {
		switch (tp) {
			case GT:
			case GTE:
			case EQ:
			case NE:
			case LTE:
			case LT:
				return 0;
			case NOT:
				return 1;
			case AND:
				return 2;
			case OR:
				return 3;
			case LEFTBRACKET:
				return 4;
			case RIGHTBRACKET:
				return 5;
			default:
				return 6;
		}
	}

	private List<Element> getInfixExpression(String str) {
		List<Element> infixExpression = new ArrayList<>();
		String[] tmp = str.replace("(", " ( ")
				.replace(")", " ) ")
				.replace(">=", "__1")
				.replace("<=", "__2")
				.replace("!=", "__3")
				.replace("<>", "__3")
				.replace(">", " > ")
				.replace("=", " = ")
				.replace("<", " < ")
				.replace("&&", " && ")
				.replace("||", " || ")
				.replace("!", " ! ")
				.replace("__1", " >= ")
				.replace("__2", " <= ")
				.replace("__3", " != ")
				.split("\\s+");
		for (String s : tmp) {
			if (s.length() == 0) {
				continue;
			}
			Element ins = new Element();
			switch (s) {
				case "(":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.LEFTBRACKET));
					break;
				case ")":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.RIGHTBRACKET));
					break;
				case ">":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.GT));
					break;
				case ">=":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.GTE));
					break;
				case "=":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.EQ));
					break;
				case "!=":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.NE));
					break;
				case "<=":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.LTE));
					break;
				case "<":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.LT));
					break;
				case "!":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.NOT));
					break;
				case "&&":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.AND));
					break;
				case "||":
					ins.setType(Type.OPERATOR);
					ins.setOperator(new Operator(OperatorType.OR));
					break;
				default:
					String op = s.toLowerCase();
					switch (op) {
						case "not":
							ins.setType(Type.OPERATOR);
							ins.setOperator(new Operator(OperatorType.NOT));
							break;
						case "and":
							ins.setType(Type.OPERATOR);
							ins.setOperator(new Operator(OperatorType.AND));
							break;
						case "or":
							ins.setType(Type.OPERATOR);
							ins.setOperator(new Operator(OperatorType.OR));
							break;
						default:
							ins.setType(Type.VALUE);
							ins.setValue(s);

					}
					break;
			}
			infixExpression.add(ins);
		}
		List<Element> ret = new ArrayList<>();
		int n = infixExpression.size();
		for (int i = 0; i < n; i++) {
			if (infixExpression.get(i).getType() == Type.OPERATOR
					&& infixExpression.get(i).getOperator().getOperatorType() == OperatorType.NOT
					&& i + 2 < n
					&& infixExpression.get(i + 1).getType() == Type.VALUE
					&& infixExpression.get(i + 2).getType() == Type.OPERATOR
					&& getOperatorPriority(infixExpression.get(i + 2).getOperator().getOperatorType()) == 0) {
				infixExpression.get(i + 2).getOperator().reverse();
			} else {
				ret.add(infixExpression.get(i));
			}
		}
		Element end = new Element();
		end.setType(Type.OPERATOR);
		end.setOperator(new Operator(OperatorType.END));
		ret.add(end);
		return ret;
	}

	public String asString(List<Element> elements) {
		StringBuilder stringBuilder = new StringBuilder("");
		for (Element element : elements) {
			if (element.getType() == Type.VALUE)
				stringBuilder.append(" " + element.getValue() + " ");
			else switch (element.getOperator().getOperatorType()) {
				case NOT:
					stringBuilder.append(" ! ");
					break;
				case AND:
					stringBuilder.append(" && ");
					break;
				case OR:
					stringBuilder.append(" || ");
					break;
				case GT:
					stringBuilder.append(" > ");
					break;
				case GTE:
					stringBuilder.append(" >= ");
					break;
				case EQ:
					stringBuilder.append(" == ");
					break;
				case NE:
					stringBuilder.append(" != ");
					break;
				case LTE:
					stringBuilder.append(" <= ");
					break;
				case LT:
					stringBuilder.append(" < ");
					break;
				case LEFTBRACKET:
					stringBuilder.append("(");
					break;
				case RIGHTBRACKET:
					stringBuilder.append(")");
					break;
				default:
					break;
			}
		}
		return stringBuilder.toString();
	}

	private List<Element> analysis(String str) {
		List<Element> infixExpr = getInfixExpression(str);
		List<Element> postfixExpr = new ArrayList<>();
		Deque<Element> operatorDeque = new ArrayDeque<>();
		for (Element e : infixExpr) {
			if (e.getType() == Type.VALUE)
				postfixExpr.add(e);
			else {
				switch (e.getOperator().getOperatorType()) {
					case LEFTBRACKET:
					case NOT:
						operatorDeque.push(e);
						break;
					case RIGHTBRACKET:
						while (!operatorDeque.isEmpty() &&
								operatorDeque.peek().getOperator().getOperatorType() != OperatorType.LEFTBRACKET) {
							postfixExpr.add(operatorDeque.pop());
						}
						operatorDeque.pop();
						if (!operatorDeque.isEmpty() &&
								operatorDeque.peek().getOperator().getOperatorType() == OperatorType.NOT) {
							postfixExpr.get(postfixExpr.size() - 1).getOperator().reverse();
							operatorDeque.pop();
						}
						break;
					case GT:
					case GTE:
					case EQ:
					case NE:
					case LTE:
					case LT:
					case AND:
					case OR:
					case END:
						int now = getOperatorPriority(e.getOperator().getOperatorType());
						while (!operatorDeque.isEmpty() &&
								getOperatorPriority(operatorDeque.peek().getOperator().getOperatorType()) <= now) {
							postfixExpr.add(operatorDeque.pop());
						}
						operatorDeque.push(e);
						break;
				}
			}
		}
		return postfixExpr;
	}

	private TreeNode build(List<Element> postfixExpr) {
		if (postfixExpr.isEmpty())
			return null;
		Deque<TreeNode> deque = new ArrayDeque<>();
		for (Element element : postfixExpr) {
			TreeNode ins = new TreeNode();
			ins.setData(element);
			if (element.getType() == Type.OPERATOR) {
				if (deque.isEmpty() || deque.size() < 2) {
					return null;
				}
				ins.setRight(deque.pop());
				ins.setLeft(deque.pop());
				if (ins.getLeft().getData().getType() == Type.VALUE) {
					ins.getLeft().getData().setType(Type.TIMESERIES);
					String tmp = ins.getLeft().getData().getValue();
					ins.getLeft().getData().setTimeseries(tmp);
				}
				deque.push(ins);
			} else {
				deque.push(ins);
			}
		}
		if (deque.size() != 1) {
			return null;
		}
		return deque.pop();
	}

	public String getBoolExpression() {
		return boolExpression;
	}

	public void setBoolExpression(String boolExpression) {
		this.boolExpression = boolExpression;
	}

	public List<Element> getPostfixExpression() {
		return postfixExpression;
	}

	public void setPostfixExpression(List<Element> postfixExpression) {
		this.postfixExpression = postfixExpression;
	}

	public TreeNode getRoot() {
		return root;
	}

	public void setRoot(TreeNode root) {
		this.root = root;
	}

	public List<String> getTimeseries() {
		return timeseries;
	}

	public void setTimeseries(List<String> timeseries) {
		this.timeseries = timeseries;
	}
}
