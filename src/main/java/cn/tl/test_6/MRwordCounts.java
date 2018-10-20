package cn.tl.test_6;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author zhangxin 6、代码实现MapReduce
 *         WordCount，其中输入路径为/tmp/tianliangedu/input_words
 *         ,输出路径为/tmp/tianliangedu/output_2018，每行的文本分隔符为空格
 */
public class MRwordCounts {

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
		job.setJarByClass(MRwordCounts.class);
		job.setMapperClass(MyTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

class MyTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private IntWritable one = new IntWritable(1);

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] s = value.toString().split(" ");
		for (String string : s) {
			word.set(string);
			context.write(word, one);
		}
	}
}

class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable value = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		value.set(sum);
		context.write(key, value);
	}
}