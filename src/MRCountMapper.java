import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MRCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//将value值使用空格进行split
		String string = value.toString();
		String[] keywords = string.split(" ");
		if (keywords!=null&&keywords.length>0) {
			for (String string2 : keywords) {
				//把单词名作为输出的Map的Key text类型
				Text outputKey = new Text(string2);
				//将map的value值直接赋值为1，intwritable 类型
				IntWritable outputVaule = new IntWritable(1);
				//将结果写入到map中
				context.write(outputKey, outputVaule);
			}
		}
	}

}
