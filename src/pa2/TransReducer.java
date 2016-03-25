package pa2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TransReducer extends Reducer<Text,Text,Text,Text>{
	//input: <author;   unigram TFIDF IDF>
	//output: <author;   unigram TFIDF IDF, unigram TFIDF IDF, ...>
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		StringBuilder build = new StringBuilder();
		for (Text val : values) {
			build.append(val + ",");
			//context.write(key, val);
		}
		
		context.write(key, new Text(build.toString()));

	}
}
