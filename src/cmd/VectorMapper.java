package cmd;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

public class VectorMapper extends Reducer<Text,Text,Text,Text> {
	public final static Path OUTPUT_PATH_UNKNOWN_NI = new Path("/output/unknownNiLocal");
	HashMap<String,Double> table = new HashMap<String,Double>();
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(OUTPUT_PATH_UNKNOWN_NI))
			return;
		FSDataInputStream in = fs.open(OUTPUT_PATH_UNKNOWN_NI);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text(""); 
		// <unigram(unknown);	TF IDF> //TODO: combine into tfidf
	
		double tfValue = 0;
		while(lineReader.readLine(currentLine) > 0){
			String[] ctnt = currentLine.toString().split("\\s+");
			tfValue = Double.parseDouble(ctnt[1]);
			table.put(ctnt[0], tfValue);		
		}
		lineReader.close();
		in.close();	
	}
}
