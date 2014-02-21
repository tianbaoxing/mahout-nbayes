package com.mahout.nb;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.training.ThetaMapper;
import org.apache.mahout.classifier.naivebayes.training.TrainNaiveBayesJob;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.base.Preconditions;

public class WriteBayesModel extends AbstractJob{

	/**
	 * @param args,输入和输出都是没有用的，输入是job1和job 2 的输出，输出是model的路径
	 * model存储的路径是 输出路径下面的naiveBayesModel.bin文件
	 * @throws ParseException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, ParseException {
		new WriteBayesModel().run(args);
	}
	/**
	 * 把model写入文件中
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public  int run(String[] args) throws IOException, ParseException{
		String[] arg={"-jt","ubuntu:9001",
				"-i","",
				"-o","",
				"-mp","hdfs://ubuntu:9000/user/mahout/output_bayes/bayesModel",
				"-bj1","hdfs://ubuntu:9000/user/mahout/output_bayes/job1",
				"-bj2","hdfs://localhost:9000/user/tianbx/mahout/out/job2/"};
		// modelPath
//        setOption("mp","modelPath",true,"the path for bayesian model to store",true);  
//        // bayes job 1 path
//        setOption("bj1","bayesJob1",true,"the path for bayes job 1",true);  
//        // bayes job 2 path
//        setOption("bj2","bayesJob2",true,"the path for bayes job 2",true);  
//		if(!parseArgs(args)){
//			return -1;
//		}
		String job1Path="hdfs://localhost:9000/user/tianbx/mahout/out/job1";
		String job2Path="hdfs://localhost:9000/user/tianbx/mahout/out/job2";
		Configuration conf=getConf();
		String modelPath="hdfs://localhost:9000/user/tianbx/mahout/out/model/";
		NaiveBayesModel naiveBayesModel=readFromPaths(job1Path,job2Path,conf);
		naiveBayesModel.validate();
	    naiveBayesModel.serialize(new Path(modelPath), getConf());
	    System.out.println("Write bayesian model to '"+modelPath+"/naiveBayesModel.bin'");
	    return 0;
	}
	/**
	 * 摘自BayesUtils的readModelFromDir方法，只修改了相关路径
	 * @param job1Path
	 * @param job2Path
	 * @param conf
	 * @return
	 */
	public  NaiveBayesModel readFromPaths(String job1Path,String job2Path,Configuration conf){
		float alphaI = conf.getFloat(ThetaMapper.ALPHA_I, 1.0f);
	    // read feature sums and label sums
	    Vector scoresPerLabel = null;//{0:3.2,1:14.099999999999998,2:75.0,3:36.0}
	    Vector scoresPerFeature = null;//{0:39.25,1:45.059999999999995,2:43.989999999999995}
	    for (Pair<Text,VectorWritable> record : new SequenceFileDirIterable<Text, VectorWritable>(
	        new Path(job2Path), PathType.LIST, PathFilters.partFilter(), conf)) {
	      String key = record.getFirst().toString();
	      VectorWritable value = record.getSecond();
	      if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_FEATURE)) {
	        scoresPerFeature = value.get();
	      } else if (key.equals(TrainNaiveBayesJob.WEIGHTS_PER_LABEL)) {
	        scoresPerLabel = value.get();
	      }
	    }

	    Preconditions.checkNotNull(scoresPerFeature);
	    Preconditions.checkNotNull(scoresPerLabel);

	    Matrix scoresPerLabelAndFeature = new SparseMatrix(scoresPerLabel.size(), scoresPerFeature.size());
//	    {
//	    	  3  =>	4:{0:11.0,1:12.0,2:13.0}
//	    	  2  =>	3:{0:22.8,1:27.299999999999997,2:24.9}
//	    	  1  =>	2:{0:4.699999999999999,1:4.7,2:4.7}
//	    	  0  =>	1:{0:0.75,1:1.06,2:1.3900000000000001}
//	    	}
	    for (Pair<IntWritable,VectorWritable> entry : new SequenceFileDirIterable<IntWritable,VectorWritable>(
	        new Path(job1Path), PathType.LIST, PathFilters.partFilter(), conf)) {
	      scoresPerLabelAndFeature.assignRow(entry.getFirst().get(), entry.getSecond().get());
	    }

	    Vector perlabelThetaNormalizer = scoresPerLabel.like();
	    return new NaiveBayesModel(scoresPerLabelAndFeature, scoresPerFeature, scoresPerLabel, perlabelThetaNormalizer,
	        alphaI);
	}
	
}
