package com.mahout.nb;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TFText2VectorWritable extends AbstractJob {
	/**
	 * 处理把
	 * 0.2	0.3	0.4：1
	 * 这样的数据转换为 key:new Text(1),value:new VectorWritable(0.2	0.3	0.4) 的序列数据
	 * @param args
	 * @throws Exception 
	 * 
	 * 正确的输出信息
		信息:     Map input records=10
		信息:     Map output records=10
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TFText2VectorWritable(),args);
	}
	@Override
	public int run(String[] args) throws Exception {
//		 addInputOption();
//	    addOutputOption();
//	    // 增加向量之间的分隔符，默认为逗号；
//	    addOption("splitCharacterVector","scv", "Vector split character,default is ','", ",");
//	    // 增加向量和标示的分隔符，默认为冒号；
//	    addOption("splitCharacterLabel","scl", "Vector and Label split character,default is ':'", ":");
//	    if (parseArguments(args) == null) {
//		      return -1;
//		}
//	    Path input = getInputPath();
//	    Path output = getOutputPath();
//	    String scv=getOption("splitCharacterVector");
//	    String scl=getOption("splitCharacterLabel");
		 Path input =new Path("hdfs://localhost:9000/user/tianbx/mahout/in/");
		 Path output = new Path("hdfs://localhost:9000/user/tianbx/mahout/out/");
		 String scv="	";
		 String scl="：";
		
	    Configuration conf=getConf();
	//    FileSystem.get(output.toUri(), conf).deleteOnExit(output);//如果输出存在，删除输出
	    HadoopUtil.delete(conf, output);
	    conf.set("SCV", scv);
	    conf.set("SCL", scl);
	    Job job=new Job(conf);
	    job.setJobName("transform text to vector by input:"+input.getName());
	    job.setJarByClass(TFText2VectorWritable.class); 
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    job.setMapperClass(TFMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(VectorWritable.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(VectorWritable.class);
	    TextInputFormat.setInputPaths(job, input);
	    SequenceFileOutputFormat.setOutputPath(job, output);
	   
	   
	    if(job.waitForCompletion(true)){
	    	return 0;
	    }
		return -1;
	}

	
	public static class TFMapper extends Mapper<LongWritable,Text,Text,VectorWritable>{
		private String SCV;
		private String SCL;
		/**
		 * 初始化分隔符参数 
		 */
		@Override
		public void setup(Context ctx){
			SCV=ctx.getConfiguration().get("SCV");
			SCL=ctx.getConfiguration().get("SCL");
		}
		/**
		 * 解析字符串，并输出
		 * @throws InterruptedException 
		 * @throws IOException 
		 */
		@Override
		public void map(LongWritable key,Text value,Context ctx) throws IOException, InterruptedException{
			String[] valueStr=value.toString().split(SCL);
			if(valueStr.length!=2){
				System.out.println("没有两个说明解析错误,退出");
				return;  
			}
			String name=valueStr[1];
			String[] vector=valueStr[0].split(SCV);
			Vector v=new RandomAccessSparseVector(vector.length);
			for(int i=0;i<vector.length;i++){
				double item=0;
				try{
					item=Double.parseDouble(vector[i]);
				}catch(Exception e){
					System.out.println("如果不可以转换，说明输入数据有问题"+vector[i]);
					return; // 如果不可以转换，说明输入数据有问题
				}
				v.setQuick(i, item);
			}
			NamedVector nv=new NamedVector(v,name);
			VectorWritable vw=new VectorWritable(nv);
			ctx.write(new Text(name), vw);
		}
		
	}
}
