package enron2;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

class MailReaderMapper extends Mapper<Text, BytesWritable, EdgeWritable, NullWritable> {

	private final EdgeWritable edgeOut = new EdgeWritable();
	private final EdgeWritable edgeIn = new EdgeWritable();
	private final NullWritable noval = NullWritable.get();
	private final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("Etc/UTC"));


	private String stripCommand(String line, String com) {
		int len = com.length();
		if (line.length() > len)
			return line.substring(len);
		return null;
	}


	private String procFrom(String line) {
		if (line == null)
			return null;
		String[] froms;
		String from = null;
		do {
			froms = line.split("\\s+|,+", 5);
			// This will only include Email accounts originating from the Enron domain
			if (froms.length == 1 && froms[0].matches(".+@enron\\.com"))
				from = froms[0];
			for (int i = 0; i < froms.length - 1; i++) {
				if (froms[i].matches(".+@enron\\.com")) {
					from = froms[i];
					break;
				}
			}
			line = froms[froms.length - 1];
		} while (froms.length > 1 && from == null);

		return from;
	}

	// TODO
	// Compile a list of recipients
	// Make sure only select Email addresses ending with
	// @enron.com
	private void procRecipients(String line, List<String> recipients) {
		Scanner sc = new Scanner(line);
		while (sc.hasNext()){
			String to = sc.next().replace(",","");

			// This will only include Email accounts originating from the Enron domain
			if (to.matches(".+@enron\\.com"))
				recipients.add(to);
		}
		sc.close();
	}
	
	// This method will return the date timestamp
	// of an email as number of milliseconds since 
	// the beginning of epoch in UTC.
	private long procDate(String stripCommand) {
		// System.out.println("stripCommand=" + stripCommand);
		try {

			cal.setTime(MailReader.sdf.parse(stripCommand.trim()));
							
		} catch (ParseException e) {
			return -1;
		}
		return (cal.get(Calendar.YEAR) >= 1998 && cal.get(Calendar.YEAR) <= 2002) ?
				cal.getTimeInMillis() : -1;
	}
	
	private static Map<String, Integer> EmailID = new HashMap<String, Integer>(); 

	@Override
	// TODO: write code to read the data from the file
	// and populate an in-memory data structure to enable
	// efficient lookup by Email address
	public void setup(Context context) throws IOException,  InterruptedException {
		Scanner s = new Scanner(new File("full-positions.csv"));
		s.nextLine();
		while(s.hasNextLine()) {
			//Declare  String variable for split
			String splitter[];
			String line = s.nextLine();
			//Split String by comma
			splitter = line.split(",");
			int ID = Integer.parseInt(splitter[0]); 
			String E1 = splitter[3].trim();
			String E2 = splitter[4].trim();
			String E3 = splitter[5].trim();
			String E4 = splitter[6].trim();
			//Add Email and ID  to HashMap
			EmailID.put(E1, ID);
			if (E2 != "") {
				EmailID.put(E2,ID);
			}
			if (E3 != "") {
				EmailID.put(E3,ID);
			}			
			if (E4 != "") {
				EmailID.put(E4,ID);
			}			
		}
		s.close();
	}

	@Override
	public void map(Text key, BytesWritable value, Context context)
			throws IOException, InterruptedException {

		byte[] bytes = value.getBytes();
		Scanner scanner = new Scanner(new ByteArrayInputStream(bytes), "UTF-8");
		String from = null; // Sender's Email address
		ArrayList<String> recipients = new ArrayList<String>(); // List of recipients' Email addresses  
		long millis = -1; // Date
		for (; scanner.hasNext(); ) {
			String line = scanner.nextLine();

			if (line.startsWith("From:")) {
				from = procFrom(stripCommand(line, "From:"));
			}
			else if (line.startsWith("To:")) {
				procRecipients(stripCommand(line, "To:"), recipients);
			}
			else if (line.startsWith("Cc:")) {
				procRecipients(stripCommand(line, "Cc:"), recipients);
			}
			else if (line.startsWith("Bcc:")) {
				procRecipients(stripCommand(line, "Bcc:"), recipients);
			}
			else if (line.startsWith("Date:")) {
				millis = procDate(stripCommand(line, "Date:"));
			}
			else if (line.startsWith("\t")) {
				procRecipients(stripCommand(line, ""), recipients);					
			}
			if (line.equals("")) { // Empty line indicates the end of the header
				break;
			}
		}
		scanner.close();

		if (from != null && recipients.size() > 0 && millis != -1) { 
			// This will fail with exception if the asserted condition
			// is false. This is a useful debugging practice.
			assert(from.endsWith("@enron.com")); 

			// TODO
			// Add your code to emit outgoing edges
			// (sender, recipient, timestamp). 
			// The edges should be emitted as keys, the values are NullWritables.
			//
			// You may use EdgeWritables edgeOut to emit 
			// an edge as a key, and noval to emit NullWritable as a value.
			
			for (String recip : recipients) {
				if (EmailID.containsKey(from)) {
					if (EmailID.containsKey(recip)) {
						if (!EmailID.get(from).equals(EmailID.get(recip))) {
							edgeOut.set(0,EmailID.get(from));
							edgeOut.set(1,EmailID.get(recip));
							edgeOut.setTS(millis);
							context.write(edgeOut,noval);
						}
					}
				}

			}
		}				
	}

	public void cleanup(Context context) throws IOException,
	InterruptedException {

	}
}