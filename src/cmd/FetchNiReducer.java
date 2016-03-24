package cmd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.LineReader;


public class FetchNiReducer extends Reducer<Text,Text,Text,Text> {	   
	//input: <unigram; 				author, TFIDF TF>
	//output: <unigram(unknown);	TF IDF> //TODO: combine into tfidf
	HashMap<String,Double> table = new HashMap<String,Double>();
	
	@Override
	public void setup(Context context) throws IOException{
	
		Path unigramFile = new Path("/output/unknownTFLocal");
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(unigramFile))
			return;
		FSDataInputStream in = fs.open(unigramFile);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text(""); 
		
	
		double tfValue = 0;
		while(lineReader.readLine(currentLine) > 0){
			String[] ctnt = currentLine.toString().split("\\s+");
			tfValue = Double.parseDouble(ctnt[1]);
			table.put(ctnt[0], tfValue);
				
		}
		lineReader.close();
		in.close();
		
	}
	
	public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
		
		double idf = 0;
		double tf = 0;
		String keyString = key.toString();
		if(table.containsKey(keyString)){
			//get idf value
			String[] cont = values.iterator().next().toString().split(",");
			String[] cont2 = cont[1].split(" ");
			idf = Double.parseDouble(cont2[1]);
			tf = table.get(key.toString());
			context.write(key, new Text("" + tf + " " + idf));
			table.remove(keyString);
		}
		
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		long numAuthor = Long.parseLong(context.getConfiguration().get("numAuthor"));
		for(Entry<String, Double> ent: table.entrySet()){
			double tf = ent.getValue();
			double idf = Math.log(numAuthor);
			context.write(new Text(ent.getKey()), new Text(tf + " " + idf +""));
		}
				
	}
  }