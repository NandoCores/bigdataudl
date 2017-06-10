package eps.examples.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.TreeMap;


public class Task3 extends Configured implements Tool {

    /**
     * Modifies the example by outputting the UserID and their name. Also does
     * not decode integers or convert to IntWritable/Texts until the cleanup
     * phase, for performance.
     *
     * @author geftimoff
     *
     */
    public static class TopTenMapper extends
            Mapper<Text,  Text, Text, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private int N;

        @Override
        protected void setup(Mapper.Context context) throws IOException,
                InterruptedException{
            Configuration conf = context.getConfiguration();
            this.N = Integer.valueOf(conf.get("N"));
        }

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {

            this.repToRecordMap.put(new Integer(value.toString()), new Text(key));

            if (repToRecordMap.size() > N*2) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer k : repToRecordMap.keySet()) {
                context.write(new Text(k.toString()), repToRecordMap.get(k));
            }
        }
    }


    public static class TopTenReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        private int N;

        @Override
        protected void setup(Reducer.Context context) throws IOException,
                InterruptedException{
            Configuration conf = context.getConfiguration();
            this.N = Integer.valueOf(conf.get("N"));
        }

        @Override
        public void reduce(Text valuesQuantity, Iterable<Text> key,
                           Context context) throws IOException, InterruptedException {

            int quantity=0;
            for (Text t : key) {
                repToRecordMap.put(new Integer(valuesQuantity.toString()), new Text(t));
                quantity++;
            }

            if (repToRecordMap.size() > N) {
                for(int i=0;i<quantity;i++){
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            for (Integer quantity : repToRecordMap.descendingKeySet()) {
                Text key = repToRecordMap.get(quantity);
                context.write(key, new Text(quantity.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new Task3(), args);
        System.exit(res);
    }



    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("N", args[2]);

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopTen");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }

        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath);

        Job job = Job.getInstance(conf, "Top Ten Patter");
        job.setJarByClass(Task3.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}