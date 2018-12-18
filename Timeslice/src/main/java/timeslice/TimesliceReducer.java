package timeslice;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class TimesliceReducer extends Reducer<Text,EdgeWritable, NullWritable, Text>{
	// You can put instance variables here to store state between iterations of
	// the reduce task.

	private final Text out = new Text();
	private final NullWritable noval = NullWritable.get();
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	
	
	// The setup method. Anything in here will be run exactly once before the
	// beginning of the reduce task.
	public void setup(Context context) throws IOException, InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	// The reducer will emit the edges (email, email, timestamp)
	// with timestamp being formatted as a normal date string
	// relative to the UTC time zone.
	public void reduce(Text key, Iterable<EdgeWritable> values, Context context)
			throws IOException, InterruptedException {			
			String basePath = key.toString();
			for (EdgeWritable val : values) {
				out.set(val.get(0) + "\t" + val.get(1)); 
				multipleOutputs.write(noval,out, basePath);	
			}
				
	}
	// The cleanup method. Anything in here will be run exactly once after the
	// end of the reduce task.
	public void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
	}

}
