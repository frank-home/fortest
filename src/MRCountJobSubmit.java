import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MRCountJobSubmit {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//获取相关参数
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		//新建job对象 选择短的那个头文件
//		Job job = Job.getInstance(conf,"word count");
		Job job = Job.getInstance(conf);
		//设置jar的起始目录
		job.setJarByClass(MRCountJobSubmit.class);
		//指定mapper程序
		job.setMapperClass(MRCountMapper.class);
		//指定Combiner程序和Reduce一样
		job.setCombinerClass(MRCountReducer.class);
		//指定Reducer程序
		job.setReducerClass(MRCountReducer.class);
		//指定输出键值类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置输入输出路径 选长的那个头文件
		FileInputFormat.addInputPath(job,new Path("hdfs://10.3.151.76:9000/user/root/input/1.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.3.151.76:9000/user/root/output/"));
		//多子job的类中，可以保证各个子job串行执行
		System.exit(job.waitForCompletion(true)?0:1);	
	}

}
