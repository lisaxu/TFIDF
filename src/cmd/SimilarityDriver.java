package cmd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import pa2.TfidfDriver;
import pa2.UnigramMapper;
import pa2.UnigramReducer;

public class SimilarityDriver {

	public static void main(String[] args) {
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
	    FileOutputFormat.setOutputPath(job1, OUTPUT_PATH_UNI);
	    if(dfs.exists(OUTPUT_PATH_UNI)) dfs.delete(OUTPUT_PATH_UNI, true);
	    code = job1.waitForCompletion(true) ? 0 : 1;

	}

}
