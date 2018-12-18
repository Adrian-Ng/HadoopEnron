package enron2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TimeZone;

class MailReaderReducer extends Reducer<EdgeWritable, NullWritable, NullWritable, Text> {
	// You can put instance variables here to store state between iterations of
	// the reduce task.

	private final Text out = new Text();
	private final NullWritable noval = NullWritable.get();

	// The setup method. Anything in here will be run exactly once before the
	// beginning of the reduce task.
	public void setup(Context context) throws IOException, InterruptedException {

	}

	// The reducer will emit the edges (email, email, timestamp)
	// with timestamp being formatted as a normal date string
	// relative to the UTC time zone.
	public void reduce(EdgeWritable key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		MailReader.sdf.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
		
		String date = MailReader.sdf.format(key.getTS());
		out.set(key.get(0) + "\t" + key.get(1) + "\t" + date);			
		context.write(noval, out);
	}

	// The cleanup method. Anything in here will be run exactly once after the
	// end of the reduce task.
	public void cleanup(Context context) throws IOException, InterruptedException {
	}
}