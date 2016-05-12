package SampleCode;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SubSetPatentMap extends  Mapper<Object, Text,Text,Text>{
	
	public void map(Object mapKey,Text mapValue,Context context) throws IOException, InterruptedException{
		Text patent = new Text();
		Text subpatent = new Text();
		
		String line = mapValue.toString();
		StringTokenizer itr = new StringTokenizer(line," ");
	    while (itr.hasMoreTokens()) {
	    	String val = itr.nextToken();
	    	patent.set(val);
	    	String subval = itr.nextToken();
	    	subpatent.set(subval);
	    	
	    	context.write(patent,subpatent);
	    	
	    }
	
		
	}

}
