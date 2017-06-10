package eps.examples.mapreduce;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;



public class Task2 extends Configured implements Tool {


    public static class MapperFilterColumn extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();

            JsonParser parser = new JsonParser();
            JsonObject object = (JsonObject)parser.parse(line);

            JsonObject entities = (JsonObject)parser.parse(object.get("entities").toString());

            JsonObject newObject = new JsonObject();
            newObject.addProperty("hashtag", entities.get("hashtags").toString());
            newObject.addProperty("text", object.get("text").toString().replace("\"", ""));
            newObject.addProperty("lang", object.get("lang").toString().replace("\"", ""));

            word.set(newObject.toString());
            output.collect(word, one);
        }
    }

    public static class MapperOneLanguage extends MapReduceBase
            implements Mapper<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        String language = "en";

        public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = key.toString();

            JsonParser parser = new JsonParser();
            JsonObject object = (JsonObject)parser.parse(line);
            JsonElement element = object.get("lang");


            if(element.toString().replace("\"", "").compareTo(language) == 0){
                word.set(line);
                output.collect(word, one);
            }
        }
    }


    public static class MapperLackingFields extends MapReduceBase
            implements Mapper<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = key.toString();

            JsonParser parser = new JsonParser();
            JsonObject object = (JsonObject)parser.parse(line);

            String strHashtag = object.get("hashtag").toString();
            String removeBracket = strHashtag.replace("[", "").replace("]", "").replace("\"", "");

            if(!removeBracket.isEmpty() && object.get("text").toString() != ""){
                word.set(line);
                output.collect(word, one);
            }
        }
    }

    public static class MapperLowerCase extends MapReduceBase
            implements Mapper<Text, IntWritable, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        String language = "en";

        public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            String line = key.toString();

            JsonParser parser = new JsonParser();
            JsonObject object = (JsonObject)parser.parse(line);

            word.set(object.get("text").toString().replace("\"", "").toLowerCase());
            output.collect(word, one);
        }
    }



    public static class WordCountReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Task2.class);
        conf.setJobName("wordcount");

        Path outputPath = new Path(args[1]);
        FileSystem  fs = FileSystem.get(new URI(outputPath.toString()), conf);
        //It will delete the output directory if it already exists. don't need to delete it  manually
        fs.delete(outputPath);

        //Setting the input and output path
        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, outputPath);

        //Considering the input and output as text file set the input & output format to TextInputFormat
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        JobConf mapAConf = new JobConf(false);
        ChainMapper.addMapper(conf, MapperFilterColumn.class, LongWritable.class, Text.class, Text.class, IntWritable.class, true, mapAConf);

        //addMapper will take global conf object and mapper class ,input and output type for this mapper and output key/value have to be sent by value or by reference and localJObconf specific to this call
        JobConf mapBConf = new JobConf(false);
        ChainMapper.addMapper(conf, MapperOneLanguage.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, mapBConf);

        JobConf mapCConf = new JobConf(false);
        ChainMapper.addMapper(conf, MapperLackingFields.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, mapCConf);

        JobConf mapDConf = new JobConf(false);
        ChainMapper.addMapper(conf, MapperLowerCase.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, mapDConf);

        JobConf reduceConf = new JobConf(false);
        ChainReducer.setReducer(conf, WordCountReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, reduceConf);

        /*JobConf mapCConf = new JobConf(false);
        ChainReducer.addMapper(conf, LastMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, true, mapCConf);
        */

        JobClient.runJob(conf);



        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task2(), args);
        System.exit(res);
    }
}