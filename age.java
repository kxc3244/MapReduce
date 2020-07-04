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
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

              String line = value.toString();
              String[] data = line.split(",");
              String serialno=data[0];
              String year=serialno.substring(0,4); //Year
              String age=data[8];
			  
			  String wagp;
			  String age1;
			  String gender;
			  Text combo;
			  
              if (age>=0 && age<=9) {
      			age1="bucket1";
      		} else if (age>=10 && age<=19) {
      			age1 = "bucket2 ";
      		} else if(age>=20 && age<=29) {
      			age1 = "bucket3"; 
		} else if(age>=30 && age<=39) {
			age1 ="bucket4";
	
		} else if(age>=40 && age<=49) {
			age1 ="bucket5";
		}  else if(age>=50 && age<=59) {
			age1 ="bucket6";
		}  else if(age>=60 && age<=69) {
			age1 ="bucket7";
		}  else if(age>=70 && age<=79) {
			age1 ="bucket8";
		}  else if(age>=80 && age<=89) {
			age1 ="bucket9";
		}  else if(age>=90 && age<=99) {
			age1 ="bucket10";
		} else {
			age1 ="INVALID";
		}
		           

              if (!age1.equals("INVALID")) {
      			int age2 = Integer.parseInt(age);
      			combo.set(new Text(age1) );
      			context.write(combo, new IntWritable(age2));
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
         }
     // sum=sum/i;
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
