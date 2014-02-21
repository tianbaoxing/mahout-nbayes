package com.mahout.nb;


import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.ResultAnalyzer;
import org.apache.mahout.classifier.naivebayes.BayesUtils;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnalyzeBayesModel extends AbstractJob{

	/**
	 * 输入是BayesClassifyJob的输出
	 * -o 参数没作用
	 */
	private static final Logger log = LoggerFactory.getLogger(AnalyzeBayesModel.class);
	public static void main(String[] args) throws IOException, ParseException {
//		String[] arg={"-jt","ubuntu:9001",
//				"-i","hdfs://ubuntu:9000/user/mahout/output_bayes/classifyJob",
//				"-o","",
//				"-li","hdfs://ubuntu:9000/user/mahout/output_bayes/index.bin"
//				};
		new AnalyzeBayesModel().run(args);
	}
	/**
	 * 分析BayesClassifyJob输出文件和labelIndex做对比，分析正确率
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public  int run(String[] args) throws IOException, ParseException{
	
		 // labelIndex
//        setOption("li","labelIndex",true,"the path where labelIndex store",true);  

//		if(!parseArgs(args)){
//			return -1;
//		}
		Configuration conf=getConf();
		String labelIndex="hdfs://localhost:9000/user/tianbx/mahout/out/index.bin";
		String input="hdfs://localhost:9000/user/tianbx/mahout/out/classify";
		Path inputPath=new Path(input);
		//load the labels
	    Map<Integer, String> labelMap = BayesUtils.readLabelIndex(getConf(), new Path(labelIndex));

	    //loop over the results and create the confusion matrix
	    SequenceFileDirIterable<Text, VectorWritable> dirIterable =
	        new SequenceFileDirIterable<Text, VectorWritable>(inputPath,
	                                                          PathType.LIST,
	                                                          PathFilters.partFilter(),
	                                                          conf);
	    ResultAnalyzer analyzer = new ResultAnalyzer(labelMap.values(), "DEFAULT");
	    analyzeResults(labelMap, dirIterable, analyzer);

	    log.info("{} Results: {}",  "Standard NB", analyzer);
	    return 0;
	}
	/**
	 * 摘自TestNaiveBayesDriver中的analyzeResults方法
	 */
	private  void analyzeResults(Map<Integer, String> labelMap,
            SequenceFileDirIterable<Text, VectorWritable> dirIterable,
            ResultAnalyzer analyzer) {
		for (Pair<Text, VectorWritable> pair : dirIterable) {
			int bestIdx = Integer.MIN_VALUE;
			double bestScore = Long.MIN_VALUE;
			for (Vector.Element element : pair.getSecond().get().all()) {
				if (element.get() > bestScore) {
					bestScore = element.get();
					bestIdx = element.index();
				}
			}
			if (bestIdx != Integer.MIN_VALUE) {
				ClassifierResult classifierResult = new ClassifierResult(labelMap.get(bestIdx), bestScore);
				analyzer.addInstance(pair.getFirst().toString(), classifierResult);
			}
		}
	}
	
}
