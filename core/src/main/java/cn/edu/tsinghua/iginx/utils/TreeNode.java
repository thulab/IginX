package cn.edu.tsinghua.iginx.utils;

public class TreeNode {
	private Element data;
	private TreeNode left;
	private TreeNode right;

	public Element getData() {
		return data;
	}

	public TreeNode getLeft() {
		return left;
	}

	public TreeNode getRight() {
		return right;
	}

	public void setData(Element data) {
		this.data = data;
	}

	public void setLeft(TreeNode left) {
		this.left = left;
	}

	public void setRight(TreeNode right) {
		this.right = right;
	}
}
