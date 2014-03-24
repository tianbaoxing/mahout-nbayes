package com.mahout.pfgrowth;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortAndCut {
	/**
	 *  sort and cut the items
	 */	
	public static class M extends Mapper<LongWritable,Text,NullWritable,Text>{
		private LinkedHashSet<String> list=new LinkedHashSet<String>();
		private Pattern splitter=Pattern.compile("[ ]*[ ,|\t]");
		
		public void setup(Context context) throws IOException{
			String input=context.getConfiguration().get("FLIST");
			 FileSystem fs=FileSystem.get(URI.create(input),context.getConfiguration());
				Path path=new Path(input);
				FSDataInputStream in1=fs.open(path);
				InputStreamReader isr1=new InputStreamReader(in1);
				BufferedReader br=new BufferedReader(isr1);
				String line;
				while((line=br.readLine())!=null){
					String[] str=line.split("\t");
					if(str.length>0){
						list.add(str[0]);
					}
				}
		}
		// map
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String [] items=splitter.split(value.toString());
			Set<String> set=new HashSet<String>();
			set.clear();
			for(String s:items){
				set.add(s);
			}
			Iterator<String> iter=list.iterator();
			StringBuffer sb=new StringBuffer();
			sb.setLength(0);
			int num=0;
			while(iter.hasNext()){
				String item=iter.next();
				if(set.contains(item)){
					sb.append(item+",");
					num++;
				}
			}
			if(num>0){
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length!=3){
			System.out.println("Usage: <input><output><fListPath>");
			System.exit(1);
		}
		String input=args[0];
		String output=args[1];
		String fListPath=args[2];
		Configuration conf=new Configuration();
		conf.set("FLIST", fListPath);
		Job job=new Job(conf,"the sort and cut  the items  job");
		job.setJarByClass(SortAndCut.class);
		job.setMapperClass(M.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);	
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));	
		boolean succeed=job.waitForCompletion(true);
		if(succeed){
			System.out.println(job.getJobName()+" succeed ... ");
		}
	}
}
