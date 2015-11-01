package com.mrako.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.*;

import javax.security.auth.callback.TextInputCallback;

/*
*  Modify this file to combine books from the same other into
*  single JSON object. 
*  i.e. {"author": "Tobias Wells", "books": [{"book":"A die in the country"},{"book": "Dinky died"}]}
*  Beaware that, this may work on anynumber of nodes! 
*
*/

public class QueryAuthor {

  public static class Map extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

      String author;
      String book;
      String line = value.toString();
      String[] tuple = line.split("\\n");
      try{
        for(int i=0;i<tuple.length; i++){
          JSONObject obj = new JSONObject(tuple[i]);
          author = obj.getString("author");
          book = obj.getString("book");
          context.write(new Text(author), new Text(book));
        }
      }catch(JSONException e){
        e.printStackTrace();
      }
    }
  }

  public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

      try {
        Configuration conf = context.getConfiguration();
        String selectAuthor = conf.get("SelectByAuthor");

        JSONObject authorObject = new JSONObject();
        JSONArray bookArray = new JSONArray();

        for(Text val : values){
          JSONObject singleBook = new JSONObject().put("book", val.toString());
          bookArray.put(singleBook);
        }

        authorObject.put("books", bookArray);
        authorObject.put("author", key.toString());

        if (selectAuthor.equals(key.toString())) {
          context.write(NullWritable.get(), new Text(authorObject.toString()));
        }

      } catch(JSONException e) {
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    if (args.length < 3) {
      System.err.println("Usage: QueryAuthor <in> <out> <author>");
      System.exit(2);
    }

    conf.set("SelectByAuthor", args[2]);

    Job job = new Job(conf, "QueryAuthor");

    job.setJarByClass(QueryAuthor.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
