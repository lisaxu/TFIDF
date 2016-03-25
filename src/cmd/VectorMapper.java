package cmd;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import common.PathUtil;

public class VectorMapper extends Mapper<Text,Text,Text,DoubleWritable> {
	
	HashMap<String,Double> table = new HashMap<String,Double>();
	double BiNorm = 0;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		//file format (from job2): <unigram(unknown) 	TFIDF> 
		//load into table all [unigram(unknown)   TFIDF], so that every mapper have them
		//calculate Bi norm as you add entries into the table
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if(!fs.exists(PathUtil.OUTPUT_FILE_UNKNOWN_TFIDF))
			return;
		FSDataInputStream in = fs.open(PathUtil.OUTPUT_FILE_UNKNOWN_TFIDF);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text(""); 
			
		double tfidfValue = 0;
		while(lineReader.readLine(currentLine) > 0){
			String[] ctnt = currentLine.toString().split("\\s+");
			tfidfValue = Double.parseDouble(ctnt[1]);
			table.put(ctnt[0], tfidfValue);
			BiNorm += tfidfValue * tfidfValue;
		}
		lineReader.close();
		in.close();	
		BiNorm = Math.sqrt(BiNorm);
		
	}
	
	//input: <author;   unigram TFIDF IDF, unigram TFIDF IDF, ...> (output from phase1 last job, AAV)
	//output: <author;	cosine_similarity> 	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] wordEntry = value.toString().split(",");
		double AiNorm = 0;
		double dotProduct = 0;
		double tfidf = 0;
		
		for(String ent: wordEntry){ 
			//"unigram TFIDF IDF"
			// [0]      [1]  [2]
			String[] cont = ent.split(" ");
			tfidf = Double.parseDouble(cont[1]);
			if(table.containsKey(cont[0])){
				dotProduct += tfidf * table.get(cont[0]);
			}
			AiNorm += tfidf * tfidf;
		}
		AiNorm = Math.sqrt(AiNorm);
		double similarity = dotProduct / (AiNorm * BiNorm);
		context.write(key, new DoubleWritable(similarity));
	}
	
}
