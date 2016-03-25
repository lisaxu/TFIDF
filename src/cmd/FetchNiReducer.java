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

import common.PathUtil;

public class FetchNiReducer extends Reducer<Text,Text,Text,Text> {	   
	
	HashMap<String,Double> table = new HashMap<String,Double>();
	
	@Override
	public void setup(Context context) throws IOException{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(PathUtil.OUTPUT_FILE_UNKNOWN_TF)) //"unigram TF"
			return;
		FSDataInputStream in = fs.open(PathUtil.OUTPUT_FILE_UNKNOWN_TF);
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
	
	
	//input: <unigram;   author|TFIDF IDF>	
	//output: <unigram(unknown);	TFIDF> 
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		double idf = 0;
		double tf = 0;
		String keyString = key.toString();
		if(table.containsKey(keyString)){
			//get idf value
			String[] cont = values.iterator().next().toString().split("\\|"); //"author|TFIDF IDF"
			String[] cont2 = cont[1].split(" "); //"TFIDF IDF"
			idf = Double.parseDouble(cont2[1]);
			tf = table.get(key.toString());
			context.write(key, new Text(tf*idf+""));
			table.remove(keyString);
		}
    }
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		long numAuthor = Long.parseLong(context.getConfiguration().get("numAuthor"));
		for(Entry<String, Double> ent: table.entrySet()){
			double tf = ent.getValue();
			double idf = Math.log(numAuthor) / Math.log(2);
			context.write(new Text(ent.getKey()), new Text(tf*idf+""));
		}
				
	}
  }