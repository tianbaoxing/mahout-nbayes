package com.mahout.pfgrowth;


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class OutRules {
	
	public static class M extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String str=value.toString();
			String[] s=str.split(",");
			if(s.length<=1){
				return;
			}
			Stack<String> stack=new Stack<String>();
			for(int i=0;i<s.length;i++){
				stack.push(s[i]);
			}
			int num=str.length();
			while(stack.size()>1){
				num=num-2;
				context.write(new Text(stack.pop()),new Text(str.substring(0,num)));
			}
		}
	}
	// Reducer
	public static class R extends Reducer<Text ,Text,Text,Text>{
		private int minConfidence=0;
		public void setup(Context context){
			String str=context.getConfiguration().get("MIN");
			try {
				minConfidence=Integer.parseInt(str);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				minConfidence=3;
			}
		}
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			HashMap<String,Integer> hm=new HashMap<String ,Integer>();
			for(Text v:values){
				String[] str=v.toString().split(",");
				for(int i=0;i<str.length;i++){
					if(hm.containsKey(str[i])){
						int temp=hm.get(str[i]);
						hm.put(str[i], temp+1);
					}else{
						hm.put(str[i], 1);
					}
				}
			}
			//  end of for
			TreeSet<String> sss=new TreeSet<String>(new SpecificComparator(" "));
			Iterator<Entry<String,Integer>> iter=hm.entrySet().iterator();
			while(iter.hasNext()){
				Entry<String,Integer> k=iter.next();
				if(k.getValue()>minConfidence&&!key.toString().equals(k.getKey())){
					sss.add(k.getKey()+" "+k.getValue());
				}
			}
			Iterator<String> iters=sss.iterator();
			StringBuffer sb=new StringBuffer();
			while(iters.hasNext()){
				sb.append(iters.next()+"|");
			}
			context.write(key, new Text(":\t"+sb.toString()));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length!=3){
			System.out.println("Usage: <input><output><min_confidence>");
			System.exit(1);
		}
		String input=args[0];
		String output=args[1];
		String minConfidence=args[2];	
		Configuration conf=new Configuration();
		conf.set("MIN", minConfidence);
		Job job=new Job(conf,"the out rules   job");
		job.setJarByClass(OutRules.class);
		job.setMapperClass(M.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(R.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));	
		boolean succeed=job.waitForCompletion(true);
		if(succeed){
			System.out.println(job.getJobName()+" succeed ... ");
		}
	}
}
