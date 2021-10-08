import java.io.IOException;
import java.util.StringTokenizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTotalAmount {

  public static class AverageTotalAmountMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private final static FloatWritable one = new FloatWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      word.set(value);
      String nextLine = word.toString();
      String [] columns = nextLine.split(",");
      if (columns.length == 8 && !columns[0].equals("pickup_time"))
      {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
          Date date = df.parse(columns[0]);
          SimpleDateFormat newdf = new SimpleDateFormat("HH");
          String datestring = newdf.format(date) + ":00-" + newdf.format(date) +":59";
          context.write(new Text(datestring), new FloatWritable(Float.parseFloat(columns[7])), one);
        } catch (ParseException e){
          e.printStackTrace();
        }
      }
    }
  }

  public static class AverageTotalAmountReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      int count = 0;
      for (FloatWritable val : values) {
        sum += val.get();
        count += 1;
      }
      if (count != 0) result.set(sum / count);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(AverageTotalAmount.class);
    job.setMapperClass(AverageTotalAmountMapper.class);
    job.setCombinerClass(AverageTotalAmountReducer.class);
    job.setReducerClass(AverageTotalAmountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
