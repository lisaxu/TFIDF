package pa2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AuthorTFWritable implements Writable{
	private Text author;
	private DoubleWritable TF;
	
	//must have a default constructor so that MR framework can instantiate them
	public AuthorTFWritable(){
		author = new Text();
		TF = new DoubleWritable();
	}
	
	public AuthorTFWritable(Text unigram, DoubleWritable TF){
		this.author = unigram;
		this.TF = TF;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		author.readFields(arg0);
		TF.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		author.write(arg0);
		TF.write(arg0);
	}
	
	public void setAuthor(Text a){
		author = a;
	}
	
	public void setAuthor(String a){
		author.set(a);
	}
	
	public void setTF(DoubleWritable f){
		TF = f;
	}
	
	public void setTF(double f){
		TF.set(f);
	}
	
	
	public double getTF(){
		return TF.get();
	}
	
	
	public Text getAuthor(){
		return author;
	}
}
