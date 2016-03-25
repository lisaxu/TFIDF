package cmd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import common.PathUtil;
import pa2.TfidfDriver;
import pa2.UnigramMapper;
import pa2.UnigramReducer;

public class SimilarityDriver {

	public static void getMaxOccurrence(Configuration conf) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		if(!fs.exists(PathUtil.OUTPUT_FILE_UNKNOWN_UNI)) 
			return;
		FSDataInputStream in = fs.open(PathUtil.OUTPUT_FILE_UNKNOWN_UNI);
		LineReader lineReader = new LineReader(in, conf);
		Text currentLine = new Text(""); 
		
		
		HashMap<String,Integer> table = new HashMap<String,Integer>();
		int freq = 0, max = Integer.MIN_VALUE;
		while(lineReader.readLine(currentLine) > 0){
			String[] ctnt = currentLine.toString().split("\\s+");
			freq = Integer.parseInt(ctnt[1]);
			table.put(ctnt[0], freq);
			if(freq > max){
				max = freq;
			}	
		}
		lineReader.close();
		in.close();
		
		//write TF value to output file
		if(fs.exists(PathUtil.OUTPUT_FILE_UNKNOWN_TF)) fs.delete(PathUtil.OUTPUT_FILE_UNKNOWN_TF,true);
		FSDataOutputStream out = fs.create(PathUtil.OUTPUT_FILE_UNKNOWN_TF);
		for(Map.Entry<String, Integer> ent: table.entrySet()){
			out.writeBytes(ent.getKey() + " " + (double)ent.getValue() / max + "\n");
		}
		out.close();
	}
	
	public static void getNumberOfAuthor(Configuration conf) throws IOException{
		Path NumAuthorFile = PathUtil.OUTPUT_PATH_N;
		FileSystem fs = NumAuthorFile.getFileSystem(conf);
		if(!fs.exists(NumAuthorFile))
			return;
		FSDataInputStream in = fs.open(NumAuthorFile);
		long numAuthor = in.readLong();
		in.close();
		conf.set("numAuthor", numAuthor+"");
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
  	      System.out.printf("You need to provide two arguments: input and output \n");
  	      System.exit(-1);
  	    }
		int code = 0;
		Configuration conf = new Configuration();
	    //get hdfs
		FileSystem dfs = FileSystem.get(conf);
		
		//job 1
		/*  */
		Job job1 = Job.getInstance(conf, "get document unigram");
	    job1.setJarByClass(TfidfDriver.class);
	    job1.setMapperClass(DocumentMapper.class);
	    job1.setCombinerClass(DocumentReducer.class);
	    job1.setReducerClass(DocumentReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, PathUtil.OUTPUT_PATH_UNKNOWN_UNI);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_UNKNOWN_UNI)) dfs.delete(PathUtil.OUTPUT_PATH_UNKNOWN_UNI, true);
	    code = job1.waitForCompletion(true) ? 0 : 1;
	    
	    getMaxOccurrence(conf);
	    getNumberOfAuthor(conf);
	    
	   
	    Job job2 = Job.getInstance(conf, "tfidf"); //fetch ni and calculate tfidf
	    //job2.addCacheFile(OUTPUT_PATH_UNKNOWN_TF.toUri()); //add unigram TF vector to distributed cache
	    job2.setJarByClass(TfidfDriver.class);
	    job2.setMapperClass(FetchNiMapper.class);
	    job2.setReducerClass(FetchNiReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, PathUtil.OUTPUT_PATH_NI);
	    FileOutputFormat.setOutputPath(job2, PathUtil.OUTPUT_PATH_UNKNOWN_TFIDF);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_UNKNOWN_TFIDF)) dfs.delete(PathUtil.OUTPUT_PATH_UNKNOWN_TFIDF, true);
	    code = job2.waitForCompletion(true) ? 0 : 1;
	    
	    Job job3 = Job.getInstance(conf, "cosine"); 
	    job3.setJarByClass(TfidfDriver.class);
	    job3.setMapperClass(VectorMapper.class);
	    job3.setReducerClass(VectorReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job3, PathUtil.OUTPUT_PATH_OFFLINE_RESULT);
	    FileOutputFormat.setOutputPath(job3, PathUtil.OUTPUT_PATH_FINAL);
	    job3.setInputFormatClass(KeyValueTextInputFormat.class);
	    job3.setOutputFormatClass(TextOutputFormat.class);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_FINAL)) dfs.delete(PathUtil.OUTPUT_PATH_FINAL, true);
	    code = job3.waitForCompletion(true) ? 0 : 1;
	    
	    System.exit(code);
	}

}
