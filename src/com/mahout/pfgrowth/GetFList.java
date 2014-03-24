package com.mahout.pfgrowth;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//  the specific comparator
class SpecificComparator implements Comparator<String>{
	private String splitter=",";
	public SpecificComparator(String splitter){
		this.splitter=splitter;
	}
	@Override
	public int compare(String o1, String o2) {
		// TODO Auto-generated method stub
		String[] str1=o1.toString().split(splitter);
		String[] str2=o2.toString().split(splitter);
		int num1=Integer.parseInt(str1[1]);
		int num2=Integer.parseInt(str2[1]);
		if(num1>num2){
			return -1;
		}else if(num1<num2){
			return 1;
		}else{
			return str1[0].compareTo(str2[0]);
		}
	}
}

public class GetFList {
	/**
	 *  the program is based on the picture 
	 */
	// Mapper
	public static class  MapperGF extends Mapper<LongWritable ,Text ,Text,IntWritable>{
		private Pattern splitter=Pattern.compile("[ ]*[ ,|\t]");
		private final IntWritable newvalue=new IntWritable(1);
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String [] items=splitter.split(value.toString());
			for(String item:items){
				context.write(new Text(item), newvalue);
			}
		}
	}
	// Reducer
	public static class ReducerGF extends Reducer<Text,IntWritable,Text ,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int temp=0;
			for(IntWritable v:value){
				temp+=v.get();
			}
			context.write(key, new IntWritable(temp));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		if(args.length!=3){
			System.out.println("Usage: <input><output><min_support>");
			System.exit(1);
		}
		String input=args[0];
		String output=args[1];
		int minSupport=0;
		try {
			minSupport=Integer.parseInt(args[2]);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			minSupport=3;
		}
		Configuration conf=new Configuration();
		String temp=args[1]+"_temp";
		Job job=new Job(conf,"the get flist job");
		job.setJarByClass(GetFList.class);
		job.setMapperClass(MapperGF.class);
		job.setCombinerClass(ReducerGF.class);
		job.setReducerClass(ReducerGF.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(temp));		
		boolean succeed=job.waitForCompletion(true);
		if(succeed){		
			//  read the temp output and write the data to the final output
			List<String> list=readFList(temp+"/part-r-00000",minSupport);
			System.out.println("the frequence list has generated ... ");
			// generate the frequence file
			generateFList(list,output);
			System.out.println("the frequence file has generated ... ");
		}else{
			System.out.println("the job is failed");
			System.exit(1);
		}				
	}
	//  read the temp_output and return the frequence list
	public static List<String> readFList(String input,int minSupport) throws IOException{
		// read the hdfs file
		Configuration conf=new Configuration();
		Path path=new Path(input);
		FileSystem fs=FileSystem.get(path.toUri(),conf);
		FSDataInputStream in1=fs.open(path);
		PriorityQueue<String> queue=new PriorityQueue<String>(15,new SpecificComparator("\t"));	
		InputStreamReader isr1=new InputStreamReader(in1);
		BufferedReader br=new BufferedReader(isr1);
		String line;
		while((line=br.readLine())!=null){
			int num=0;
			try {
					num=Integer.parseInt(line.split("\t")[1]);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				num=0;
			}
			if(num>minSupport){
				queue.add(line);
			}
		}
		br.close();
		isr1.close();
		in1.close();
		List<String> list=new ArrayList<String>();
		while(!queue.isEmpty()){
			list.add(queue.poll());
		}
		return list;
	}
	// generate the frequence file
	public static void generateFList(List<String> list,String output) throws IOException{
		Configuration conf=new Configuration();
		Path path=new Path(output);
		FileSystem fs=FileSystem.get(path.toUri(),conf);
		FSDataOutputStream writer=fs.create(path);
		Iterator<String> i=list.iterator();
		while(i.hasNext()){
			writer.writeBytes(i.next()+"\n");//  in the last line add a \n which is not supposed to exist
		}
		writer.close();
	}
}
