package com.mahout.pfgrowth;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PFGrowth_ParallelCountingR extends 
			Reducer<Text,LongWritable,Text,LongWritable>{
	
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,
    		InterruptedException {
		long sum = 0;
		for (LongWritable value : values) {
		context.setStatus("Parallel Counting Reducer :" + key);
		sum += value.get();
		}
		context.setStatus("Parallel Counting Reducer: " + key + " => " + sum);
		context.write(key, new LongWritable(sum));
	}
}
