package com.mahout.similarity;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import com.mahout.in.action.recommender.FileUtil;

/**文件intro.csv:
 *  1,101,5.0
	1,102,3.0
	1,103,2.5
	
	2,101,2.0
	2,102,2.5
	2,103,5.0
	2,104,2.0
	
	3,101,2.5
	3,104,4.0
	3,105,4.5
	3,107,5.0
	
	4,101,5.0
	4,103,3.0
	4,104,4.5
	4,106,4.0
	
	5,101,4.0
	5,102,3.0
	5,103,2.0
	5,104,4.0
	5,105,3.5
	5,106,4.0
 * */
public class TestUserSimilarity {

	public static void main(String[] args) throws IOException, TasteException {
		 DataModel model = new FileDataModel(new File(FileUtil.recommender_path+"intro.csv"));
//		 UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
//		 //查看其他用户和用户1的相识度
//		 System.err.println("测试PearsonCorrelation相似值=======================================================");
//		 System.out.println("用户2和用户1的相似值是:"+similarity.userSimilarity(1, 2));
//		 System.out.println("用户3和用户1的相似值是:"+similarity.userSimilarity(1, 3));
//		 System.out.println("用户4和用户1的相似值是:"+similarity.userSimilarity(1, 4));
//		 System.out.println("用户5和用户1的相似值是:"+similarity.userSimilarity(1, 5));
//		 
//		 UserSimilarity similarityWithWEIGHTED = new PearsonCorrelationSimilarity(model,Weighting.WEIGHTED);
//		 //查看其他用户和用户1的相识度
//		 System.err.println("测试加权的PearsonCorrelation相似值=======================================================");
//		 System.out.println("用户2和用户1的相似值是:"+similarityWithWEIGHTED.userSimilarity(1, 2));
//		 System.out.println("用户3和用户1的相似值是:"+similarityWithWEIGHTED.userSimilarity(1, 3));
//		 System.out.println("用户4和用户1的相似值是:"+similarityWithWEIGHTED.userSimilarity(1, 4));
//		 System.out.println("用户5和用户1的相似值是:"+similarityWithWEIGHTED.userSimilarity(1, 5));
//		 
		 
		 UserSimilarity euclideanDistanceSimilarity= new EuclideanDistanceSimilarity(model);
		 //查看其他用户和用户1的相识度
		 System.err.println("测试euclideanDistanceSimilarity相似值=======================================================");
		 System.out.println("用户2和用户1的相似值是:"+euclideanDistanceSimilarity.userSimilarity(1, 2));
		 System.out.println("用户3和用户1的相似值是:"+euclideanDistanceSimilarity.userSimilarity(1, 3));
		 System.out.println("用户4和用户1的相似值是:"+euclideanDistanceSimilarity.userSimilarity(1, 4));
		 System.out.println("用户5和用户1的相似值是:"+euclideanDistanceSimilarity.userSimilarity(1, 5));
		 
		 
	}

}



