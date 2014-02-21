package com.mahout.nb;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.training.WeightsMapper;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
/**
 * 用于分类的Job
 * @author Administrator
 *
 */
public class BayesClassifyJob extends AbstractJob {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BayesClassifyJob(),args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
//		 addInputOption();
//	    addOutputOption();
//	    addOption("model","m", "The file where bayesian model store ");
//	    addOption("labelNumber","ln", "The labels number ");
//	    if (parseArguments(args) == null) {
//		      return -1;
//		}
//	    Path input = getInputPath();
//	    Path output = getOutputPath();
//	    String labelNumber=getOption("labelNumber");
//	    String modelPath=getOption("model");
		 Path input = new Path("hdfs://localhost:9000/user/tianbx/mahout/out/part-m-00000");
		 Path output = new Path("hdfs://localhost:9000/user/tianbx/mahout/out/classify");
		 String labelNumber="4";
		String modelPath="hdfs://localhost:9000//user/tianbx/mahout/out/model/";
	    Configuration conf=getConf();
	    conf.set(WeightsMapper.class.getName() + ".numLabels",labelNumber);
	    HadoopUtil.cacheFiles(new Path(modelPath), conf);
	    HadoopUtil.delete(conf, output);
	    Job job=new Job(conf);
	    job.setJobName("Use bayesian model to classify the  input:"+input.getName());
	    job.setJarByClass(BayesClassifyJob.class); 
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setMapperClass(BayesClasifyMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(VectorWritable.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
	    SequenceFileInputFormat.setInputPaths(job, input);
	    SequenceFileOutputFormat.setOutputPath(job, output);
	    
	    if(job.waitForCompletion(true)){
	    	return 0;
	    }
		return -1;
	}
	/**
	 *  自定义Mapper，只修改了解析部分代码
	 * @author Administrator
	 *
	 */
	public static class BayesClasifyMapper extends Mapper<Text, VectorWritable, Text, VectorWritable>{
		private AbstractNaiveBayesClassifier classifier;
			@Override
		  public void setup(Context context) throws IOException, InterruptedException {
		    System.out.println("Setup");
		    Configuration conf = context.getConfiguration();
		    Path modelPath = HadoopUtil.getSingleCachedFile(conf);
		    NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
		    classifier = new StandardNaiveBayesClassifier(model);
		  }

		  @Override
		  public void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
		    Vector result = classifier.classifyFull(value.get());
		    //the key is the expected value
		    context.write(new Text(key.toString()), new VectorWritable(result));
		  }
	}
}
