package cn.tl.test_5;

import java.io.File;

/**
 * @author zhangxin 5.
 *         在hdfs中存在一个文本文件路径/tmp/tianliangedu/input/student.txt文件，试读取其文件内容
 *         ，打印到控制台当中。（只写代码即可）
 */
public class ReadTxt {

	public static void main(String[] args) throws Exception {
		String path1 = "" + File.separator + "tmp" + File.separator
				+ "tianliangedu" + File.separator + "input" + File.separator
				+ "student.txt";
		String content = HDFSFileOperatorUtil.readFileToString(path1);
		System.out.println(content);
	}

}
