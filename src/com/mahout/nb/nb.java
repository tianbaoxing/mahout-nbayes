package com.mahout.nb;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;

public class nb {
	
	public static void main(String[] args) throws IOException {
		String filename = "D:\\git\\mahout-nbayes\\src\\data.txt";
		String scv="	";
		String scl="：";
		FileInputStream inputStream = new FileInputStream(filename);
		InputStreamReader streamReader = new InputStreamReader(inputStream);
		BufferedReader bufferedReader = new BufferedReader(streamReader);
		String line  = null;
//		0.2	0.3	0.4：1
//		0.32	0.43	0.45：1
//		0.23	0.33	0.54：1
//		2.4	2.5	2.6：2
//		2.3	2.2	2.1：2
//		5.4	7.2	7.2：3
//		5.6	7	6：3
//		5.8	7.1	6.3：3
//		6	6	5.4：3
//		11	12	13：4
		List<NamedVector> namedVectorList = new ArrayList<NamedVector>();
		while((line=bufferedReader.readLine())!=null){
			String[] valueStr=line.toString().split(scl);
			if(valueStr.length!=2){
				System.out.println("没有两个说明解析错误,退出");
				return;  
			}
			String name=valueStr[1];
			String[] vector=valueStr[0].split(scv);
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
//			VectorWritable vw=new VectorWritable(nv);
//			ctx.write(new Text(name), vw);
			namedVectorList.add(nv);
			System.err.println(nv.toString());
		}
		//打印结果
//		1:{0:0.2,1:0.3,2:0.4}
//		1:{0:0.32,1:0.43,2:0.45}
//		1:{0:0.23,1:0.33,2:0.54}
//		2:{0:2.4,1:2.5,2:2.6}
//		2:{0:2.3,1:2.2,2:2.1}
//		3:{0:5.4,1:7.2,2:7.2}
//		3:{0:5.6,1:7.0,2:6.0}
//		3:{0:5.8,1:7.1,2:6.3}
//		3:{0:6.0,1:6.0,2:5.4}
//		4:{0:11.0,1:12.0,2:13.0}
		
		
		Collection<String> seen = new HashSet<String>();
		Map<Text,IntWritable> textAndInt = new HashMap<Text, IntWritable>();
	    int i = 0;
        for (Object label : namedVectorList) {//(1,1:{0:0.2,1:0.3,2:0.4})
	        String theLabel = ((NamedVector)label).getName().toString();
	        if (!seen.contains(theLabel)) {
	//	          writer.append(new Text(theLabel), new IntWritable(i++));
	          textAndInt.put(new Text(theLabel), new IntWritable(i++));
	          seen.add(theLabel);
	        }
        }
        System.err.println(textAndInt);
	    System.out.println("labels number is : "+i);

	    
	    System.out.println("job1-----------------------------------");
	    Vector vector = null;
	    Map<IntWritable, Vector> intAndVector = new HashMap<IntWritable, Vector>();
	    for (NamedVector v : namedVectorList) {
	    	vector = intAndVector.get(textAndInt.get(new Text(v.getName())));
	      if (vector == null) {
	        vector = v.getDelegate();
	        intAndVector.put(textAndInt.get(new Text(v.getName())), vector);
	      } else {
	        vector.assign(v.getDelegate(), Functions.PLUS);
	      }
	    }
	    System.err.println(intAndVector);
	    
	    System.out.println("job2-----------------------------------");
	    Vector weightsPerFeature = null;
	    Vector weightsPerLabel = null;
	    Vector vector__SPF = null;
	    Vector vector__SPL = null;
	    for(IntWritable key : intAndVector.keySet()){
	    	 Vector instance = intAndVector.get(key);
	    	 weightsPerLabel = new DenseVector(i);
	 	     weightsPerFeature  = new RandomAccessSparseVector(instance.size(), instance.getNumNondefaultElements());
	 	     System.err.println("instance="+instance+"|instance.zSum()"+instance.zSum());
	 	     weightsPerLabel.set(key.get(), weightsPerLabel.get(key.get()) + instance.zSum());
	 	     
	 	     weightsPerFeature.assign(instance, Functions.PLUS);
	 	     if(vector__SPF==null){
	 	    	vector__SPF = weightsPerFeature;
	 	     }else{
	 	    	vector__SPF.assign(weightsPerFeature, Functions.PLUS);
	 	     }
	 	    if(vector__SPL==null){
	 	    	vector__SPL = weightsPerLabel;
	 	     }else{
	 	    	vector__SPL.assign(weightsPerLabel, Functions.PLUS);
	 	     }
	    }
	    System.err.println("vector__SPF="+vector__SPF);//各个分类所有元素的综合
	    System.err.println("vector__SPL="+vector__SPL);
//	    vector__SPF={0:39.25,1:45.059999999999995,2:43.989999999999995}
//	    vector__SPL={0:3.2,1:14.099999999999998,2:75.0,3:36.0}
	    System.out.println("model------------------------------------------------------");
	    float alphaI = 1.0f;
	    Matrix scoresPerLabelAndFeature = new SparseMatrix(vector__SPL.size(), vector__SPF.size());
//	    {
//	    	  3  =>	4:{0:11.0,1:12.0,2:13.0}
//	    	  2  =>	3:{0:22.8,1:27.299999999999997,2:24.9}
//	    	  1  =>	2:{0:4.699999999999999,1:4.7,2:4.7}
//	    	  0  =>	1:{0:0.75,1:1.06,2:1.3900000000000001}
//	    	}
	    for(IntWritable key : intAndVector.keySet()){
	    	 Vector instance = intAndVector.get(key);
	    	 scoresPerLabelAndFeature.assignRow(key.get(), instance);
	    }

	    System.err.println("scoresPerLabelAndFeature="+scoresPerLabelAndFeature);
	    Vector perlabelThetaNormalizer = vector__SPL.like();
	    System.err.println("perlabelThetaNormalizer="+perlabelThetaNormalizer);
	    NaiveBayesModel bayesModel = new NaiveBayesModel(scoresPerLabelAndFeature, vector__SPF, vector__SPL, perlabelThetaNormalizer,
	        alphaI);
	    System.err.println("bayesModel="+bayesModel);
	    
	    
	    System.out.println("classify------------------------------------------------");
	    AbstractNaiveBayesClassifier  classifier =
	    	new StandardNaiveBayesClassifier(bayesModel);
	    
	    
	    for(Vector vector2 : namedVectorList){
	    	Vector result = classifier.classifyFull(vector2);
	    	System.err.println("vector2="+vector2+"[result]="+result);
	    }
	    
	    
	   
	    
		
	}
}
