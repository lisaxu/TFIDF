package pa2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MaxMapper extends Mapper <Text, Text, Text, UnigramFreqWritable>{
	//input: <author|unigram;	termFrequencyFij>
	//output: <author;   UnigramFreqWritable:[unigram termFrequencyFij]>
	
	UnigramFreqWritable outpt = new UnigramFreqWritable();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		String[] authorUnigram = key.toString().split("\\|");	
		outpt.setFreq(value);
		outpt.setUnigram(authorUnigram[1]);
		context.write(new Text(authorUnigram[0]), outpt);
	}
}