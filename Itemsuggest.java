import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Itemsuggest {

	public static class ItempairMapper
       		extends Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	//private Text word = new Text();
	private HashMap<Text,ItemMapWritable> hashmap;//a hashmap for each item

	//init
	@Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            hashMap = new HashMap();
        }

	//functions for Item hashmap update
	//MapWritable for sub map of each item
	public static class ItemMapWritable extends MapWritable {
		@Override
        	public String toString(){
            		String s = new String("{ ");
            		Set<Writable> keyitems = this.keySet();
            		for (Writable key : keyitems) {
				IntWritable count = (IntWritable) this.get(key);
         			s = s + key.toString() + " has " + count.toString() + ", ";
          	  	}
         		s = s + " }";
			return s;
		}
	}

	//function map based on map of WordCount
	//Using hashmap for <item,ItemMapWritable> where ItemMapWritable using a map for count
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			hashMap.put(new Text(itr.nextToken()),new ItemMapWritable());
		}
		for (HashMap.Entry<Text,ItemMapWritable> entry: hashMap.entrySet()) {
                	addAnotherItem(entry.getKey());
		}	
	}
	
	private void addAnotherItem(Text t){
            for (HashMap.Entry<Text,ItemMapWritable> entry: hashMap.entrySet()) {
                if (!entry.getKey().equals(t)){
                    MyMapWritable m = entry.getValue();
                    m.put(new Text(t.toString()),one);
                }
            }
        }


	//function reduce NOT finished
	public static class ItemlistReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}


  //main function based on WordCount
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "item suggest");
    job.setJarByClass(Itemsuggest.class);
    job.setMapperClass(ItempairMapper.class);
    job.setReducerClass(ItemlistReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
