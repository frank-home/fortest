import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MRCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text arg0, Iterable<IntWritable> arg1,
			Context arg2) throws IOException, InterruptedException {
		//把value值进行累积叠加
		int result=0;
		for (IntWritable intWritable : arg1) {
			result=result+intWritable.get();
		}
		//输入Map的Key作为输出Map的Key/value 累加求和后的结果作为输出map的value
		arg2.write(arg0, new IntWritable(result));	
		
	}
	
}
