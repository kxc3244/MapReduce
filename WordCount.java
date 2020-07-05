import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text combo = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

              String line = value.toString();
              String[] data = line.split(",");
              String serialno=data[0];
              String year=serialno.substring(0,4); //Year
              String state=data[5];//State
      	      String wage=data[72];//Wage
			  
			  String wagp;
			  String gender;
			  
              if (wage.equals("bbbbbb")) {
      			wagp="Ignore";
      		} else if (wage.equals("000000")) {
      			wagp = "Ignore ";
      		} else {
      			wagp = "others"; }
              String gender1=data[69];//**************************************
              if(gender1.equals("1"))
              {
              	gender="Male";
              }
              else
              {
              	gender="Female";
              }
              int year1;
      	if(year.equals("2009"))
      	{
      		year1=2009;
      	}else if(year.equals("2010")) {
                  	year1=2010;
      	}else if(year.equals("2011")){
      		year1=2011;
      	}else if(year.equals("2012")){
      		year1=2012;
      	}else if(year.equals("2013")){
      		year1=2013;
      	}else {
      		year1=000;
      	}

              if (!wagp.equals("Ignore") && year1!=000) {
      			int wage1 = Integer.parseInt(wage);
      			combo.set(new Text(year) + "," +new Text(gender) + "," + new Text(wage));
      			context.write(combo, new IntWritable(wage1));
              }

    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      int i = 0;
      for (IntWritable val : values) {
        sum += val.get();
        i++;
      }
      sum=sum/i;
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}