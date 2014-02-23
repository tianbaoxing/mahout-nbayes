package com.mahout.pfgrowth;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
public class PFGrowth_ParallelCounting {
	public boolean runParallelCountingJob(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		Job job = new Job(conf, "Parallel Counting Driver running over input: " + input);
	    job.setJarByClass(PFGrowth_ParallelCounting.class);
	    job.setMapperClass(PFGrowth_ParallelCountingM.class);
	    job.setCombinerClass(PFGrowth_ParallelCountingR.class);
	    job.setReducerClass(PFGrowth_ParallelCountingR.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class); // &nbsp;get rid of this line you can get the text file
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);    
	    FileInputFormat.setInputPaths(job,new Path( input));
	    Path outPut=new Path(output,"parallelcounting");
	    HadoopUtil.delete(conf, outPut);
	    FileOutputFormat.setOutputPath(job, outPut);	    
	    boolean succeeded = job.waitForCompletion(true);
	    if (!succeeded) {
	      throw new IllegalStateException("Job failed!");
	    }	
		return succeeded;
	}
}

