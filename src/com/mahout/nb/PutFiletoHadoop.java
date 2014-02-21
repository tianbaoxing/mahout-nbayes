package com.mahout.nb;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
public class PutFiletoHadoop {

	public static void main(String[] args) {
		try {
			upLoadToCloud("data.txt", "data.txt");
//			deleteFile("hdfs://localhost:9000/user/tianx");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void deleteFile(String file) throws IOException{
	        // 获取一个conf对象
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(file), conf);
	        fs.deleteOnExit(new Path(file));
	}

	 private static void upLoadToCloud(String srcFileName, String cloudFileName)
	            throws FileNotFoundException, IOException {
	        // 本地文件存取的位置
	        String LOCAL_SRC = "/home/tianbx/workspace/mahout-nb/src/" + srcFileName;
	        // 存放到云端HDFS的位置
	        String CLOUD_DEST = "hdfs://localhost:9000/user/tianbx/mahout/in/" + cloudFileName;
	        InputStream in = new BufferedInputStream(new FileInputStream(LOCAL_SRC));
	        // 获取一个conf对象
	        Configuration conf = new Configuration();
	        // 文件系统
	        FileSystem fs = FileSystem.get(URI.create(CLOUD_DEST), conf);
	        // 输出流
	        OutputStream out = fs.create(new Path(CLOUD_DEST), new Progressable() {
	            @Override
	            public void progress() {
	                System.out.println("上传完成一个文件到HDFS");
	            }
	        });
	        // 连接两个流，形成通道，使输入流向输出流传输数据
	        IOUtils.copyBytes(in, out, 1024, true);
	    }
}
