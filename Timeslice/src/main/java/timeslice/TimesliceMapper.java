package timeslice;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class TimesliceMapper extends Mapper<Object, Text, Text, EdgeWritable> {

	private final EdgeWritable edgeOut = new EdgeWritable();
	private final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("Etc/UTC"));
	// This method will return the date timestamp
	// of an email as number of milliseconds since 
	// the beginning of epoch in UTC.
	private long procDate(String date) {
		// System.out.println("stripCommand=" + stripCommand);
		try {

			cal.setTime(Timeslice.sdf.parse(date.trim()));

		} catch (ParseException e) {
			return -1;
		}
		return (cal.get(Calendar.YEAR) >= 1998 && cal.get(Calendar.YEAR) <= 2002) ?
				cal.getTimeInMillis() : -1;
	}
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			String s = value.toString();
			String splitter[];
			splitter = s.split("\t");
			Long millis = procDate(splitter[2]);
			String month = new SimpleDateFormat("yyyy-MM").format(millis);
			Text monthOut = new Text(month);
			edgeOut.set(0,Integer.parseInt(splitter[0]));
			edgeOut.set(1,Integer.parseInt(splitter[1]));
			context.write(monthOut,edgeOut);		
	}

	public void cleanup(Context context) throws IOException,
	InterruptedException {

	}

}
