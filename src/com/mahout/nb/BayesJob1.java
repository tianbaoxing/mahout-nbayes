package com.mahout.nb;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
/**
 * 贝叶斯算法第一个job任务相当于 TrainNaiveBayesJob的第一个prepareJob
 * 只用修改Mapper即可，Reducer还用原来的
 * @author Administrator
 * 
 * 正确的结果:
 *  Map output records=10
 *	Reduce output records=4
 */
public class BayesJob1 extends AbstractJob {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BayesJob1(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
//		 addInputOption();
//	    addOutputOption();
//	    addOption("labelIndex","li", "The path to store the label index in");
//	    if (parseArguments(args) == null) {
//		      return -1;
//		}
//	    Path input = getInputPath();
//	    Path output = getOutputPath();
//	    String labelPath=getOption("labelIndex");
		Path input = new Path("hdfs://localhost:9000/user/tianbx/mahout/out/part-m-00000");
	    Path output = new Path("hdfs://localhost:9000/user/tianbx/mahout/out/job1/");
	    String labelPath="hdfs://localhost:9000/user/tianbx/mahout/out/index.bin";
	    Configuration conf=getConf();
	    HadoopUtil.cacheFiles(new Path(labelPath), getConf());
	    HadoopUtil.delete(conf, output);
	    Job job=new Job(conf);
	    job.setJobName("job1 get scoreFetureAndLabel by input:"+input.getName());
	    job.setJarByClass(BayesJob1.class); 
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setMapperClass(BJMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(VectorWritable.class);
	    job.setCombinerClass(VectorSumReducer.class);
	    job.setReducerClass(VectorSumReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(VectorWritable.class);
	    SequenceFileInputFormat.setInputPaths(job, input);
	    SequenceFileOutputFormat.setOutputPath(job, output);
	    
	    if(job.waitForCompletion(true)){
	    	return 0;
	    }
		return -1;
	}
	/**
	 * 自定义Mapper，只是解析的地方有改动而已
	 * @author Administrator
	 *
	 */
	public static class BJMapper extends Mapper<Text, VectorWritable, IntWritable, VectorWritable>{
		public enum Counter { SKIPPED_INSTANCES }

		  private OpenObjectIntHashMap<String> labelIndex;//[4->3, 3->2, 2->1, 1->0]

		  @Override
		  protected void setup(Context ctx) throws IOException, InterruptedException {
		    super.setup(ctx);
		    labelIndex = BayesUtils.readIndexFromCache(ctx.getConfiguration()); //
		  }

		  @Override
		  protected void map(Text labelText, VectorWritable instance, Context ctx) throws IOException, InterruptedException {
		    String label = labelText.toString(); 
		    if (labelIndex.containsKey(label)) {
		      ctx.write(new IntWritable(labelIndex.get(label)), instance);
		    } else {
		      ctx.getCounter(Counter.SKIPPED_INSTANCES).increment(1);
		    }
		  }
	}

}
