
//hadoop jar TeleTrans.jar  WireFraud.FATM_MultiCard5000 -Dmapreduce.job.queuename=root.spark TeleTrans/quxian_1606 TeleTrans/saveFATM_5000_1606 


package WireFraud;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import WireFraud.MultiSortUtil;

public class FATM_MultiCard5000 {
 

	public static class UpdateMapper extends Mapper<LongWritable, Text, MultiSortUtil.KeyPair, Text> {

		public MultiSortUtil.KeyPair keyPair = new MultiSortUtil.KeyPair();
		private Text info = new Text();
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	String[] list = itr.nextToken().split("\\001");
            	
            if(list.length == 9){
            	String card= list[0];
            	String time = list[2] + list[3];
            	String trans_md = list[5];
            	String isCross = list[6];
            	String ATM = list[7] + list[8];

                String money = list[1];
            	String newstr = ATM + " " + time + " " + money + " " + card + " " + isCross + " " + trans_md;
                keyPair.set(ATM, time); 
            	info.set(newstr);
            	
            }
    			context.write(keyPair, info);
            }
		}
	}

	public static class UpdateReducer extends Reducer<MultiSortUtil.KeyPair, Text, Text, Text> {
		private Text result = new Text();
		
		@Override
		public void reduce(MultiSortUtil.KeyPair key, Iterable<Text> lines, Context context)
				throws IOException, InterruptedException {
		    String curtime ="";
			String lasttime = "";
			 
			String curCard ="";
			String lastCard = "";
			int cardDiff = 0;

			for (Text info : lines) {
				if(!info.toString().isEmpty()){
				  String[] list = info.toString().split("\\s");
				  String ATM = list[0];
				  String money = list[2];
				  String card = list[3];
				  String isCross = list[4];
				  String trans_md = list[5];
	              String curTS = list[1];
	              curCard = card;
				  
	              //异地  大额  cardDiff区分是否换卡
	              if(curTS.length()==14 && trans_md.equals("2") && Double.valueOf(money)>=500000){	
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
				    
				    if(curCard.equals(lastCard))
				    	cardDiff = 0;
				    else
				    	cardDiff = 1;
				    
				    String tempInfo = ATM + "\t" + pdate + "\t" + money + "\t"+ card + "\t" + isCross + "\t" +trans_md + "\t" + DeltaT + "\t" + cardDiff;
 	
				    lastCard = curCard;
				    lasttime = curtime;
				   
				    result.set( tempInfo);
				    context.write(result,new Text(""));
			}
			}
			}
		}
	}
	
	 
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		 
		Configuration conf = new Configuration();
		//conf.set("mapreduce.job.queuename", "root.default");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = new Job(conf,"ForeignATMSort");

		job.setJarByClass(ForeignATMSort.class); // 设置运行jar中的class名称
		
		job.setPartitionerClass(MultiSortUtil.FirstPartitioner.class);
		job.setGroupingComparatorClass(MultiSortUtil.GroupingComparator.class);
		
		job.setMapperClass(UpdateMapper.class);// 设置mapreduce中的mapper reducer
		job.setReducerClass(UpdateReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(MultiSortUtil.KeyPair.class);
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
