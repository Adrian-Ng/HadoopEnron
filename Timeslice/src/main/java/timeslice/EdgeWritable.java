package timeslice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;


public class EdgeWritable  implements WritableComparable<EdgeWritable> {

	private IntWritable[] vxs = new IntWritable[] {new IntWritable(), new IntWritable()}; 
	private LongWritable timestamp = new LongWritable();

	public EdgeWritable() {

	}

	public EdgeWritable(EdgeWritable ew) {
		vxs[0].set(ew.vxs[0].get());
		vxs[1].set(ew.vxs[1].get());
		timestamp.set(ew.timestamp.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		vxs[0].readFields(in);
		vxs[1].readFields(in);
		timestamp.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vxs[0].write(out);
		vxs[1].write(out);
		timestamp.write(out);
	}

	public int get(int i) {
		return vxs[i].get();
	}

	public long getTS() {
		return timestamp.get();
	}

	public void set(int i, int w) {
		vxs[i].set(w);
	}

	public void setTS(long c) {
		timestamp.set(c);
	}

	@Override
	public String toString() {
		return vxs[0] + "\t" + vxs[1] + "\t" + timestamp.get();
	}

	@Override
	public int hashCode() {
		return vxs[0].hashCode() * 163 * 163 + vxs[1].hashCode() * 163 + timestamp.hashCode();
	}

	@Override
	public boolean equals(Object o) { 
		if (o instanceof EdgeWritable) {
			EdgeWritable ew = (EdgeWritable) o;
			return vxs[0].equals(ew.vxs[0]) && vxs[1].equals(ew.vxs[1]) && timestamp.equals(ew.timestamp);
		}
		return false; 
	}

	@Override
	public int compareTo(EdgeWritable e) {
		int cmp;
		if ((cmp = vxs[0].compareTo(e.vxs[0])) != 0)
			return cmp;
		if ((cmp = vxs[1].compareTo(e.vxs[1])) != 0)
			return cmp;
		return timestamp.compareTo(e.timestamp);
	}

}
