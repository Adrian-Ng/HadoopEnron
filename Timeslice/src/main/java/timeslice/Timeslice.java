package timeslice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class Timeslice extends Configured implements Tool {

    static final SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");


    public static void printUsage(Tool tool, String extraArgsUsage) {
        System.err.printf("Usage: %s [genericOptions] %s\n\n",
                tool.getClass().getSimpleName(), extraArgsUsage);
        GenericOptionsParser.printGenericCommandUsage(System.err);
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            printUsage(this, "<input> <output>");
            return 1;
        }

        Configuration config = getConf();

        FileSystem fs = FileSystem.get(config);

        Job job = Job.getInstance(config);
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Delete old output if necessary
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath))
            fs.delete(outPath, true);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(EdgeWritable.class);

        job.setMapperClass(TimesliceMapper.class);
        job.setReducerClass(TimesliceReducer.class);

        job.setNumReduceTasks(22);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Timeslice(), args);
        System.exit(exitCode);
    }

}
