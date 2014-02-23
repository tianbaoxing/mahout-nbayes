package com.mahout.pfgrowth;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class PFGrowth_ParallelCountingM extends 
						Mapper<LongWritable,Text,Text,LongWritable> {
	 private static final LongWritable ONE = new LongWritable(1);
	  private Pattern splitter=Pattern.compile("[ ,\t]*[ ,|\t][ ,\t]*");
	  @Override
	  protected void map(LongWritable offset, Text input, Context context) throws IOException,
	                                                                      InterruptedException {
	    String[] items = splitter.split(input.toString());
	    for (String item : items) {
	      if (item.trim().isEmpty()) {
	        continue;
	      }
	      context.setStatus("Parallel Counting Mapper: " + item);
	      context.write(new Text(item), ONE);
	    }
	  }  
}
