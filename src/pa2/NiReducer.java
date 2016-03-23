package pa2;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NiReducer extends Reducer<Text,Text,Text,Text>{
	//input: <unigram;   author,TF>
	//output: <unigram;   author,TFIDF>
	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		List<String> cache = new LinkedList<String>();
		
		Configuration conf = context.getConfiguration();
		long numAuthor = Long.parseLong(conf.get("num_author"));
		long niCount = 0;
		for (Text val : values) {
			niCount++;
			cache.add(val.toString());
		}
		double niCountDbl = niCount;
		double idf = 0;
		double tf = 0;
		double tfidf = 0;
		
		for(String s: cache){
			idf = Math.log(numAuthor / niCountDbl)/Math.log(2);
			String[] cont = s.split(",");
			tf = Double.parseDouble(cont[1]);
			tfidf = tf * idf;
			context.write(key, new Text(cont[0] + "," + tfidf));
		}
		
      
    }
}
