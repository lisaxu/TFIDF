package pa2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TransMapper extends Mapper <Text, Text, Text, Text>{
	//input: <unigram;   author|TFIDF IDF>
	//output: <author;   unigram TFIDF IDF>
	
	Text outKey = new Text(); //for author
	Text outValue = new Text();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] con = value.toString().split("\\|");
		outKey.set(con[0]);
		outValue.set(key + " " + con[1]);
		context.write(outKey, outValue);
	}
}
