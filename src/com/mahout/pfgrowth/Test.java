package com.mahout.pfgrowth;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.FileLineIterable;
import org.apache.mahout.common.iterator.StringRecordIterator;
import org.apache.mahout.fpm.pfpgrowth.CountDescendingPairComparator;
import org.apache.mahout.fpm.pfpgrowth.convertors.ContextStatusUpdater;
import org.apache.mahout.fpm.pfpgrowth.convertors.TopKPatternsOutputConverter;
import org.apache.mahout.fpm.pfpgrowth.convertors.TransactionIterator;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mahout.source.FPGrowth;

public class Test {

	 private static final Logger log = LoggerFactory.getLogger(Test.class);
	 
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		FPGrowthDriver.main(args);
		String filename = "D:\\git\\mahout-nbayes\\data\\retail.dat";
		FileInputStream inputStream = new FileInputStream(filename); 
		Charset encoding = Charset.forName("UTF-8");
		String pattern = "[ ,\t]*[ ,|\t][ ,\t]*";
//		InputStreamReader streamReader = new InputStreamReader(inputStream);
//		BufferedReader bufferedReader = new BufferedReader(streamReader);
//		String line  = null;
		
		Collection<String> returnableFeatures = Sets.newHashSet();
		FPGrowth<String> fp = new FPGrowth<String>();
		
		FileLineIterable fileLineIterable = new FileLineIterable(inputStream, encoding, false);
//		Iterator<String> iterator = fileLineIterable.iterator();
//		while(iterator.hasNext()){
//			String value = iterator.next();
//			System.out.println(value);
//		}
		StringRecordIterator transactionStream =  
				new StringRecordIterator(fileLineIterable, pattern);
		long minSupport = 5;
		
		List<Pair<String,Long>> frequencyList = generateFList(transactionStream, (int)minSupport);
		
		
	    Map<Integer,String> reverseMapping = Maps.newHashMap();
		Map<String,Integer> attributeIdMapping = Maps.newHashMap();
		
		int id = 0;
	    for (Pair<String,Long> feature : frequencyList) {
	    	String attrib = feature.getFirst();
	      Long frequency = feature.getSecond();
	      if (frequency >= minSupport) {
	        attributeIdMapping.put(attrib, id);
	        reverseMapping.put(id++, attrib);
	      }
	    }
	    
	    long[] attributeFrequency = new long[attributeIdMapping.size()];
	    for (Pair<String,Long> feature : frequencyList) {
	    	String attrib = feature.getFirst();
	      Long frequency = feature.getSecond();
	      if (frequency < minSupport) {
	        break;
	      }
	      attributeFrequency[attributeIdMapping.get(attrib)] = frequency;
	    }
	    System.out.println("Number of unique items "+frequencyList.size());
	    
	    Collection<Integer> returnFeatures = Sets.newHashSet();
	    if (returnableFeatures != null && !returnableFeatures.isEmpty()) {
	      for (String attrib : returnableFeatures) {
	        if (attributeIdMapping.containsKey(attrib)) {
	          returnFeatures.add(attributeIdMapping.get(attrib));
	          log.info("Adding Pattern {}=>{}", attrib, attributeIdMapping
	            .get(attrib));
	        }
	      }
	    } else {
	      for (int j = 0; j < attributeIdMapping.size(); j++) {
	        returnFeatures.add(j);
	      }
	    }
	   int k = 512 ;
//	   Configuration conf = new Configuration();
//	   Path output = new Path("output.txt");
//	   FileSystem fs = FileSystem.get(output.toUri(), conf);
	   
//	   SequenceFile.Writer writer =	
//		   new SequenceFile.Writer(fs, conf, output, Text.class, TopKStringPatterns.class);
	   fp.generateTopKFrequentPatterns(
			   new TransactionIterator<String>(transactionStream,attributeIdMapping), 
	           attributeFrequency, 
	           minSupport, 
	           k, 
	           reverseMapping.size(), 
	           returnFeatures, 
	           new TopKPatternsOutputConverter<String>(null,reverseMapping),  
	           new ContextStatusUpdater(null));
		System.out.println("transactionStream="+transactionStream.toString());
	}
	
	
	public static final List<Pair<String,Long>> generateFList(Iterator<Pair<List<String>,Long>> transactions, int minSupport) {

	    Map<String,MutableLong> attributeSupport = Maps.newHashMap();
	    while (transactions.hasNext()) {
	      Pair<List<String>,Long> transaction = transactions.next();
	      for (String attribute : transaction.getFirst()) {
	        if (attributeSupport.containsKey(attribute)) {
	          attributeSupport.get(attribute).add(transaction.getSecond().longValue());
	        } else {
	          attributeSupport.put(attribute, new MutableLong(transaction.getSecond()));
	        }
	      }
	    }
	    List<Pair<String,Long>> fList = Lists.newArrayList();
	    for (Entry<String,MutableLong> e : attributeSupport.entrySet()) {
	      long value = e.getValue().longValue();
	      if (value >= minSupport) {
	        fList.add(new Pair<String,Long>(e.getKey(), value));
	      }
	    }

	    Collections.sort(fList, new CountDescendingPairComparator<String,Long>());

	    return fList;
	  }

}
