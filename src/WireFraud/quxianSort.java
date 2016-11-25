//hadoop jar TeleTrans.jar  WireFraud.quxianSort -Dmapreduce.job.queuename=root.spark TeleTrans/trans0405 TeleTrans/Delta0405


package WireFraud;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class quxianSort {
	private static Logger logger = LoggerFactory
			.getLogger(SecondarySort.class);

	// 自己定义的key类应该实现WritableComparable接口
	public static class KeyPair implements WritableComparable<KeyPair> {
		String first;  //第一排序字段
		String second; //第二排序字段

		public KeyPair() {}

		//Set the left and right values.
		public void set(String left, String right) {
			first = left;
			second = right;
		}

		public String getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		/**
		 * 反序列化，从流中的二进制转换成KeyPair
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readUTF();
			second = in.readUTF();
		}

		/**
		 * 序列化，将KeyPair转化成使用流传送的二进制
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(first);
			out.writeUTF(second);
		}

		/**
		 * key的比较
		 */
		@Override
		public int compareTo(KeyPair o) {
			if (!first.equals(o.first)) {
				return first.compareTo(o.first);
			} else {
				// 由大到小排序
				return o.second.compareTo(second);
			}
		}

		// 新定义类应该重写的两个方法
		@Override
		// The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
		public int hashCode() {
			return first.hashCode() + second.hashCode();
		}

		@Override
		public boolean equals(Object right) {
			if (right == null)
				return false;
			if (this == right)
				return true;
			if (right instanceof KeyPair) {
				KeyPair kp = (KeyPair) right;
				return kp.first.equals(first) && kp.second.equals(second);
			} else {
				return false;
			}
		}
	}

	/**
	 * 分区函数类。根据first确定Partition。
	 */
	public static class FirstPartitioner extends Partitioner<KeyPair, Text> {
		@Override
		public int getPartition(KeyPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst().hashCode()) % numPartitions;
		}
	}

	/**
	 * 分组函数类。只要first相同就属于同一个组。
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(KeyPair.class, true);
		}

		// Compare two WritableComparables.
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			KeyPair kp1 = (KeyPair) w1;
			KeyPair kp2 = (KeyPair) w2;
			String kp1First = kp1.getFirst();
			String kp2First = kp2.getFirst();
			return kp1First.compareTo(kp2First);
		}
	}

	public static class UpdateMapper extends Mapper<LongWritable, Text, KeyPair, Text> {

		public KeyPair keyPair = new KeyPair();
		private Text info = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	String[] list = itr.nextToken().split("\\001");
            	
            if(list.length == 9){	
            	String card= list[0];
            	String time = list[2] + list[3];
            	String isCross = list[6];
            	String ATM = list[7] + list[8];
            	keyPair.set(card, time);
               
            	String money = list[1];
            	String newstr = card + " " + time + " " + money + " " + ATM + " " + isCross;
            	info.set(newstr);
            	
            }
    			context.write(keyPair, info);
            }
		}
	}

	public static class UpdateReducer extends Reducer<KeyPair, Text, Text, Text> {
		private Text result = new Text();
		
		@Override
		public void reduce(KeyPair key, Iterable<Text> lines, Context context)
				throws IOException, InterruptedException {
		    String curtime ="";
			String lasttime = "";
			 
	    	Double curM = 0.0;

			for (Text info : lines) {
				  String[] list = info.toString().split("\\s");
				  String card = list[0];
				  String money = list[2];
				  String ATM = list[3];
				  String isCross = list[4];
	              String curTS = list[1];
	              curM = Double.valueOf(list[2]);
				  
	              if(curTS.length()==14){	
					curtime = curTS.substring(0, 4) + "-" + curTS.substring(4, 6) + "-" + curTS.substring(6, 8) +
		      				  " " + curTS.substring(8, 10) + ":" + curTS.substring(10, 12) + ":" + curTS.substring(12, 14);
	 
					DecimalFormat df = new DecimalFormat("######0.00"); 
					
					String DeltaT = null;
			
				    if(lasttime.equals("")){
				    	DeltaT = null;
				    }
		      		else{
		      		   	Double deltaTime = getDeltaMin(curtime,lasttime);
				    	DeltaT = df.format(deltaTime);
		      		}
				    
				    String pdate = curTS.substring(0, 8);
				    
				    String tempInfo = card + "\t" + pdate + "\t" + money + "\t"+ ATM + "\t" + isCross + "\t" + DeltaT;
 	
				    lasttime = curtime;
			 
				   
				    result.set( tempInfo);
				    context.write(result,new Text(""));
			}
			}
		}
	}
	
	 
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();
		//conf.set("mapreduce.job.queuename", "root.default");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"quxianSort");

		job.setJarByClass(SecondarySort.class); // 设置运行jar中的class名称
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		
		job.setMapperClass(UpdateMapper.class);// 设置mapreduce中的mapper reducer
		job.setReducerClass(UpdateReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(KeyPair.class);
		job.setNumReduceTasks(100);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
  	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	
	public static Double getDeltaMin(String time1, String time2){
		Date date1;	
		Date date2;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			date1 = sdf.parse(time1);
			date2 = sdf.parse(time2);
			return (double) Math.abs((date1.getTime()-date2.getTime()))/(1000*60);//1000*60*60*24
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
