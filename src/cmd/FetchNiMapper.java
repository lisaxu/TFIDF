package cmd;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FetchNiMapper extends Mapper <Text, Text, Text, Text>{
	
	//input: <unigram; author, TFIDF TF>
	//output: <unigram; author, TFIDF TF> //same
	
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, value);
		
	}
}