package pa2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import common.PathUtil;



public class TfidfDriver {
	
	public static enum NUM_AUTHOR{
		COUNT
	};
	
	

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
		Job job1 = Job.getInstance(conf, "get unigram");
	    job1.setJarByClass(TfidfDriver.class);
	    job1.setMapperClass(UnigramMapper.class);
	    job1.setCombinerClass(UnigramReducer.class);
	    job1.setReducerClass(UnigramReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, PathUtil.OUTPUT_PATH_UNI);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_UNI)) dfs.delete(PathUtil.OUTPUT_PATH_UNI, true);
	    code = job1.waitForCompletion(true) ? 0 : 1;
	    
	    //job 2
	    Job job2 = Job.getInstance(conf, "calculate max");
	    job2.setJarByClass(TfidfDriver.class);
	    job2.setMapperClass(MaxMapper.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(UnigramFreqWritable.class);
	    //job2.setCombinerClass(MaxReducer.class);
	    job2.setReducerClass(MaxReducer.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, PathUtil.OUTPUT_PATH_UNI);
	    FileOutputFormat.setOutputPath(job2, PathUtil.OUTPUT_PATH_MAX);
	    job2.setInputFormatClass(KeyValueTextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_MAX)) dfs.delete(PathUtil.OUTPUT_PATH_MAX, true);
	    code = job2.waitForCompletion(true) ? 0 : 1;
	    
	    //get counters from job2
	    Counters counters = job2.getCounters();
	    Counter c = counters.findCounter(NUM_AUTHOR.COUNT);
	    long numAuthor = c.getValue();
	    //write N to a hdfs file
	    if(dfs.exists(PathUtil.OUTPUT_PATH_N)) dfs.delete(PathUtil.OUTPUT_PATH_N, true);
	    FSDataOutputStream out = dfs.create(PathUtil.OUTPUT_PATH_N);
	    out.writeLong(numAuthor);
	    out.close();
	    
	    
	    //job 3
	    Configuration tfConf = new Configuration();
	    tfConf.set("num_author", numAuthor+"");
	    
	    Job job3 = Job.getInstance(tfConf, "get ni"); 
	    job3.setJarByClass(TfidfDriver.class);
	    job3.setMapperClass(NiMapper.class);
	    job3.setMapOutputKeyClass(Text.class);
	    job3.setMapOutputValueClass(Text.class);
	    //job3.setCombinerClass(NiReducer.class);
	    job3.setReducerClass(NiReducer.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, PathUtil.OUTPUT_PATH_MAX);
	    FileOutputFormat.setOutputPath(job3, PathUtil.OUTPUT_PATH_NI);
	    job3.setInputFormatClass(KeyValueTextInputFormat.class);
	    job3.setOutputFormatClass(TextOutputFormat.class);
	    if(dfs.exists(PathUtil.OUTPUT_PATH_NI)) dfs.delete(PathUtil.OUTPUT_PATH_NI, true);
	    code = job3.waitForCompletion(true) ? 0 : 1;
	    
		//job 4
		Job job4 = Job.getInstance(tfConf, "transform");
		job4.setJarByClass(TfidfDriver.class);
		job4.setMapperClass(TransMapper.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
	    //job4.setCombinerClass(TransReducer.class);
		job4.setReducerClass(TransReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job4, PathUtil.OUTPUT_PATH_NI);
	    FileOutputFormat.setOutputPath(job4, new Path(args[1]));
	    job4.setInputFormatClass(KeyValueTextInputFormat.class);
	    job4.setOutputFormatClass(TextOutputFormat.class);
	    if(dfs.exists(new Path(args[1]))) dfs.delete(new Path(args[1]), true);
	    code = job4.waitForCompletion(true) ? 0 : 1;
		
	    System.exit(code);
	}

}
