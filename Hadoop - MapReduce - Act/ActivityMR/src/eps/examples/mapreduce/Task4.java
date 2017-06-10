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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class Task4 extends Configured implements Tool {

    public static class SentimentMapper extends Mapper<Text, Text, Text, Text> {

        private Set positiveWords = new HashSet();
        private Set negativeWords = new HashSet();

        @Override
        protected void setup(Mapper.Context context)
                  throws IOException,InterruptedException{
            URI filePositive = context.getCacheFiles()[0];
            URI fileNegative = context.getCacheFiles()[1];

            readFilePositive(filePositive.getPath().substring(filePositive.getPath().lastIndexOf('/') + 1));
            readFileNegative(fileNegative.getPath().substring(fileNegative.getPath().lastIndexOf('/') + 1));
        }

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = key.toString().split(" ") ;
            int positive=0, negative=0;

            List<String> listHashtag = new ArrayList<String>();

            for (String str: words){
                if(!str.isEmpty() && str.toCharArray()[0] == '#'){
                    listHashtag.add(str);
                }
                if(positiveWords.contains(str)){
                    System.out.println(str + " " + key);
                    positive++;
                }
                if(negativeWords.contains(str)){
                    negative--;
                }
            }

            String values = (positive+negative)+"_"+key.getLength();
            for(String hashtag : listHashtag){
                context.write(new Text(hashtag),
                              new Text(values));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        }

        private void readFilePositive(String filePath) {
            try{
                System.out.println("[StopWords] READING FILE --> " + filePath);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    positiveWords.add(stopWord.toLowerCase());
                }
            } catch(IOException ex) {
                System.err.println("[StopWords] Exception while reading stop words file: " + ex.getMessage());
            }
        }

        private void readFileNegative(String filePath) {
            try{
                System.out.println("[StopWords] READING FILE --> " + filePath);
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    negativeWords.add(stopWord.toLowerCase());
                }
            } catch(IOException ex) {
                System.err.println("[StopWords] Exception while reading stop words file: " + ex.getMessage());
            }
        }
    }


    public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void setup(Reducer.Context context) throws IOException,
                InterruptedException{
        }

        @Override
        public void reduce(Text hashtag, Iterable<Text> key,
                           Context context) throws IOException, InterruptedException {
            double numerator = 0;
            double denominator = 0;

            Boolean a = false;
            for (Text strKey : key) {
                String[] vector = strKey.toString().split("_");
                numerator += Double.valueOf(vector[0]) * Integer.valueOf(vector[1]);
                denominator += Double.valueOf(vector[1]);
            }

            System.out.println(numerator + " " + denominator);
            double division = numerator*1.0000/denominator*1.0000;
            System.out.println("div: " + division);
            context.write(new Text(hashtag), new Text(String.valueOf(division)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new Task4(), args);
        System.exit(res);
    }



    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: Sentiment Analysis");
            ToolRunner.printGenericCommandUsage(System.err);
            System.exit(2);
        }

        Path outputPath = new Path(otherArgs[1]);
        FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath);


        Job job = Job.getInstance(conf, "Sentiment Analysis");
        job.setJarByClass(Task4.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.addCacheFile(new Path(otherArgs[2]).toUri());
        job.addCacheFile(new Path(otherArgs[3]).toUri());


        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }
}