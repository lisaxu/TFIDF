package cmd;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DocumentMapper extends Mapper <Object, Text, Text, IntWritable>{
	//input: <lineoffset#, a line>
	//output: <unigram; 1>
	
	private final static IntWritable one = new IntWritable(1);
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		int start = line.indexOf("<===>");
		if(start == -1) return;
		
		
    	String[] unigram = line.substring(start + 5).replaceAll("[^A-Za-z0-9\\s]", "").toLowerCase().split("\\s+"); 
		for(String s: unigram){
			if(!s.isEmpty()){
				context.write(new Text(s), one);
			}
		}	
	}
}