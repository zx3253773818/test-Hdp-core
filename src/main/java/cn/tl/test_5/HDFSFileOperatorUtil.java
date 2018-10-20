package cn.tl.test_5;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author zhangxin 5.
 *         在hdfs中存在一个文本文件路径/tmp/tianliangedu/input/student.txt文件，试读取其文件内容
 *         ，打印到控制台当中。（只写代码即可）
 */
public class HDFSFileOperatorUtil {
	Logger loger = Logger.getLogger(HDFSFileOperatorUtil.class);
	static Configuration hadoopConf = new Configuration();

	public static byte[] readFileToByteArray(String filePath) throws Exception {
		byte[] result = null;
		if (filePath == null || filePath.trim().length() == 0) {
			throw new Exception("文件路径不对" + filePath + "，请检查");
		}
		FSDataInputStream hdfsIS = null;
		ByteArrayOutputStream baos = null;
		try {
			FileSystem fs = FileSystem.get(hadoopConf);
			Path hdfsPath = new Path(filePath);
			hdfsIS = fs.open(hdfsPath);
			byte[] b = new byte[65536];
			baos = new ByteArrayOutputStream();
			int flag = -1;
			while ((flag = hdfsIS.read(b)) != -1) {
				baos.write(b);
				// baos.write(flag);//不成功
				b = new byte[65536];
			}
			result = baos.toByteArray();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			CloseUtil.close(hdfsIS);
			CloseUtil.close(baos);
		}

		return result;
	}

	public static String readFileToString(String filePath) throws Exception {
		String result = null;
		result = new String(readFileToByteArray(filePath), "utf-8");
		return result;
	}
}
