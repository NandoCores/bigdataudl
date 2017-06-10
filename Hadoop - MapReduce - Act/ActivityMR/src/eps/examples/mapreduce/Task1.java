package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by master on 6/5/17.
 */
public class Task1 extends Configured implements Tool
{
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set(RegexMapper.PATTERN, "(?:\\s|\\A|^)[##]+([A-Za-z0-9-_]+)");

        args = new GenericOptionsParser(conf, args).getRemainingArgs();


        Path outputPath = new Path("/user/tan/Act1Output1");
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Task1.class);
        // Set Mapper class as hadoop provided TokenCounterMapper class.
        job.setMapperClass(RegexMapper.class);
        // Set Reducer class as hadoop provided IntSumReducer class.
        job.setReducerClass(LongSumReducer.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Task1(), args);
        System.exit(exitCode);
    }
}

