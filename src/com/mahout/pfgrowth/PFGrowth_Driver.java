package com.mahout.pfgrowth;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import com.google.common.collect.Lists;
class MyComparator implements Comparator<Pair<String,Long>>{
	 @Override
     public int compare(Pair<String,Long> o1, Pair<String,Long> o2) {
       int ret = o2.getSecond().compareTo(o1.getSecond());
       if (ret != 0) {
         return ret;
       }
       return o1.getFirst().compareTo(o2.getFirst());
     }	
}
public class PFGrowth_Driver {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException{
		if(args.length!=3){
			System.out.println("wrong input args");
			System.out.println("usage: <intput><output><minsupport>");
			System.exit(-1);
		}
		// set parameters
		Parameters params=new Parameters();
		params.set("INPUT", args[0]);
		params.set("OUTPUT", args[1]);
		params.set("MIN_SUPPORT", args[2]);
		// get parameters
		String input=params.get("INPUT");
		String output=params.get("OUTPUT");
		//  run the first job
		PFGrowth_ParallelCounting ppc=new PFGrowth_ParallelCounting();
		ppc.runParallelCountingJob(input, output);	
		//  read input and set the fList
		 List<Pair<String,Long>> fList = readFList(params);
		 Configuration conf=new Configuration();
		 saveFList(fList, params, conf);		 
	}	
	/**
	   * Serializes the fList and returns the string representation of the List
	   * 
	   * @return Serialized String representation of List
	   */
	  public static void saveFList(Iterable<Pair<String,Long>> flist, Parameters params, Configuration conf)
	    throws IOException {
	    Path flistPath = new Path(params.get("OUTPUT"), "fList");
	    FileSystem fs = FileSystem.get(flistPath.toUri(), conf);
	    flistPath = fs.makeQualified(flistPath);
	    HadoopUtil.delete(conf, flistPath);
	    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, flistPath, Text.class, LongWritable.class);
	    try {
	      for (Pair<String,Long> pair : flist) {
	        writer.append(new Text(pair.getFirst()), new LongWritable(pair.getSecond()));
	      }
	    } finally {
	      writer.close();
	    }
	    DistributedCache.addCacheFile(flistPath.toUri(), conf);
	  }
	public static List<Pair<String,Long>> readFList(Parameters params) {
	    int minSupport = Integer.valueOf(params.get("MIN_SUPPORT"));
	    Configuration conf = new Configuration();    
	    Path parallelCountingPath = new Path(params.get("OUTPUT"),"parallelcounting");
	    //  add MyComparator
	    PriorityQueue<Pair<String,Long>> queue = new PriorityQueue<Pair<String,Long>>(11,new MyComparator());
	    // sort according to the occur times from large to small 

	    for (Pair<Text,LongWritable> record
	         : new SequenceFileDirIterable<Text,LongWritable>(new Path(parallelCountingPath, "part-*"),
	                                                        PathType.GLOB, null, null, true, conf)) {
	      long value = record.getSecond().get();
	      if (value >= minSupport) {   // get rid of the item which is below the minimum support
	        queue.add(new Pair<String,Long>(record.getFirst().toString(), value));
	      }
	    }
	    List<Pair<String,Long>> fList = Lists.newArrayList();
	    while (!queue.isEmpty()) {
	      fList.add(queue.poll());
	    }
	    return fList;
	  }	
}
