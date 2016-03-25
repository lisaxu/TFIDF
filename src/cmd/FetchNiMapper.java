package cmd;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FetchNiMapper extends Mapper <Text, Text, Text, Text>{
	
	//input: <unigram;   author|TFIDF IDF>
	//output: <unigram;   author|TFIDF IDF> //same
	//it is output from pahse1 job3, because its key is unigram, easy to look up words without split
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, value);
		
	}
}