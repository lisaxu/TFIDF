package pa2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import pa2.TfidfDriver.NUM_AUTHOR;

public class MaxReducer extends Reducer<Text,UnigramFreqWritable,Text,Text> {	   
	//input: <author;   UnigramFreqWritable:[unigram termFrequencyFij]>
	//output: <unigram;   author,TF>
	
	public void reduce(Text key, Iterable<UnigramFreqWritable> values,Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> cache = new HashMap<String, Integer>();
		
    	
    	int max = Integer.MIN_VALUE;
    	int freq = -1;
    	for (UnigramFreqWritable val : values) {  //cannot iterate twice, cache it
    	  freq = val.getFreq();
    	  if(freq > max){
    		  max = freq;
    	  }
    	 
    	  cache.put(val.getUnigram().toString(), freq);
    	}
    	
    	for(Map.Entry<String, Integer> ent: cache.entrySet()){
    		 context.write(new Text(ent.getKey()), new Text(key.toString() + "," +  (double)ent.getValue() / max));
    	}
    	

    	context.getCounter(NUM_AUTHOR.COUNT).increment(1);
    }
    
	
    public void cleanup(Context context){
    	//???WTF?? clean up is called once at the end of task
    	//context.getCounter(NUM_AUTHOR.COUNT).increment(1);
    }
  }
