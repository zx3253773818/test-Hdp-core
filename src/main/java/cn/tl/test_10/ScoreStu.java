package cn.tl.test_10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin
 *         10、在hdfs目录/tmp/table/score中存在score.txt文件，按tab分隔，字段名为(学号，姓名，班级
 *         ，分数）。求每个班级成绩Top5。
 */
public class ScoreStu {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration hdpConf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(hdpConf, args);
		String[] remainingArgs = gop.getRemainingArgs();
		if (remainingArgs.length != 2) {
			System.err
					.println("Usage: yarn jar jar_path main_class_path -D 参数列表 <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(hdpConf, "wordCounts");
		job.setJarByClass(ScoreStu.class);
		job.setMapperClass(MyTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private IntWritable one = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] s = value.toString().split("\\t");

		word.set(s[2]);
		one.set(Integer.parseInt(s[3]));
		context.write(word, one);
	}
}

class IntSumReducer extends Reducer<Text, IntWritable, Text, Text> {
	Text nv = new Text();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		List<Integer> list = new ArrayList<Integer>();
		for (IntWritable intWritable : values) {
			list.add(intWritable.get());
		}
		Collections.sort(list);
		nv.set(list.get(0) + "\t" + list.get(1) + "\t" + list.get(2) + "\t"
				+ list.get(3) + "\t" + list.get(4));
		context.write(key, nv);
	}
}