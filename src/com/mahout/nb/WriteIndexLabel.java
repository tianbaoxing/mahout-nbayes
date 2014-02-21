package com.mahout.nb;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;

import com.google.common.io.Closeables;
/**
 * 主要操作是把输入文件的所有标识全部读取出来，然后进行转换，转换为数值型
 * 
 * 正确输出结果:labels number is : 4
	4
 * */
public class WriteIndexLabel {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String inputPath="hdfs://localhost:9000/user/tianbx/mahout/out/part-m-00000";
		String labPath="hdfs://localhost:9000/user/tianbx/mahout/out/index.bin";
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "ubuntu:9001");
		long t=writeLabelIndex(inputPath,labPath,conf);
		System.out.println(t);
	}
	/**
	 * 从输入文件中读出全部标识，并加以转换,然后写入文件
	 * @param inputPath
	 * @param labPath
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static long writeLabelIndex(String inputPath,String labPath,Configuration conf) throws IOException{
		long labelSize=0;
		Path p=new Path(inputPath);
		Path lPath=new Path(labPath);
		SequenceFileDirIterable<Text, IntWritable> iterable =
	              new SequenceFileDirIterable<Text, IntWritable>(p, PathType.LIST, PathFilters.logsCRCFilter(), conf);
		labelSize = writeLabel(conf, lPath, iterable);
		return labelSize;
	}
	
	/**
	 * 把数字和标识的映射写入文件
	 * @param conf
	 * @param indexPath
	 * @param labels
	 * @return
	 * @throws IOException
	 */
	public static long writeLabel(Configuration conf,Path indexPath,Iterable<Pair<Text,IntWritable>> labels) throws IOException{
		FileSystem fs = FileSystem.get(indexPath.toUri(), conf);
	    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, indexPath, Text.class, IntWritable.class);
	    Collection<String> seen = new HashSet<String>();
	    int i = 0;
	    try {
	      for (Object label : labels) {//(1,1:{0:0.2,1:0.3,2:0.4})
	        String theLabel = ((Pair<?,?>) label).getFirst().toString();
	        if (!seen.contains(theLabel)) {
	          writer.append(new Text(theLabel), new IntWritable(i++));
	          seen.add(theLabel);
	        }
	      }
	    } finally {
	      Closeables.closeQuietly(writer);
	    }
	    System.out.println("labels number is : "+i);
	    return i;
	}
}
