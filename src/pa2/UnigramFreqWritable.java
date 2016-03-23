package pa2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class UnigramFreqWritable implements Writable {
	private Text unigram;
	private Text freq;
	
	//must have a default constructor so that MR framework can instantiate them
	public UnigramFreqWritable(){
		unigram = new Text();
		freq = new Text();
	}
	
	public UnigramFreqWritable(Text unigram, Text freq){
		this.unigram = unigram;
		this.freq = freq;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		unigram.readFields(arg0);
		freq.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		unigram.write(arg0);
		freq.write(arg0);
	}
	
	public void setUnigram(Text uni){
		unigram = uni;
	}
	
	public void setUnigram(String uni){
		unigram.set(uni);
	}
	
	public void setFreq(Text f){
		freq = f;
	}
	
	
	public int getFreq(){
		return Integer.parseInt(freq.toString());
	}
	
	public Text getFreqText(){
		return freq;
	}
	
	public Text getUnigram(){
		return unigram;
	}

}
