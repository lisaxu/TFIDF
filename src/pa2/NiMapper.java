package pa2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NiMapper extends Mapper <Text, Text, Text, Text>{
	//input: <unigram;   author|TF>
	//output: <unigram;   author|TF>
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		context.write(key, value);
	}
}


