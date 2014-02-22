package com.mahout.source;

import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

public class CustomStandardNaiveBayesClassifier extends StandardNaiveBayesClassifier{
	 public CustomStandardNaiveBayesClassifier(NaiveBayesModel model) {
		super(model);
	}

	@Override
	  public double getScoreForLabelFeature(int label, int feature) {
	    NaiveBayesModel model = getModel();
	    double value = computeWeight(model.weight(label, feature), model.labelWeight(label), model.alphaI(),
	        model.numFeatures());
	    return value ;
	  }

	  public static double computeWeight(double featureLabelWeight, double labelWeight, double alphaI,
	      double numFeatures) {
	    double numerator = featureLabelWeight + alphaI;
	    double denominator = labelWeight + alphaI * numFeatures;
	    double value = Math.log(numerator / denominator);
	    return value ;
	  }
	  
	  protected double getScoreForLabelInstance(int label, Vector instance) {
		    double result = 0.0;
		    double eget = 0.0;
		    for (Element e : instance.nonZeroes()) {
		    	eget = e.get();
		      result += eget * getScoreForLabelFeature(label, e.index());
		    }
		    return result;
		  }
}
