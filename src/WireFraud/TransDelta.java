//hadoop jar TransDelta.jar  TransDelta -Dmapreduce.job.queuename=root.default TeleTrans/TransSort/000000_0 TeleTrans/Deltatest
//hadoop jar TransDelta.jar  TransDelta -Dmapreduce.job.queuename=root.default TeleTrans/trans0405 TeleTrans/Delta0405
//数据量太大的话，reducer里面尽量不要用List  treeset 这类的东西。
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TransDelta {

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
	
    public static class CardMapper extends Mapper<Object,Text,Text,Text>{
        private Text card = new Text();
        private Text info = new Text();
        @Override
		public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
            StringTokenizer itr=new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
            	String[] list = itr.nextToken().split("\\001");
            	card.set(list[0]);
            	info.set(list[3] + " " + list[4] + " " + list[2]);
            
                context.write(card, info);
            }
        }
    }

    public static class DeltaReducer extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        
        @Override
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        	TreeSet<String> treeset = new TreeSet<String>();     //写成TreeSet<Text>就不行。因为Text只有在mapreduce的输入输出阶段有用貌似
        	for (Text val : values) {
        		treeset.add(val.toString());
            }
        	
        	Iterator<String> iter = treeset.iterator();
        	
        	ArrayList<Double> Moneylist = new ArrayList<Double>();
        	ArrayList<HashMap<String, Double>> Infolist = new ArrayList<HashMap<String, Double>>();
        	
        	
        	String lasttime = "";
        	String curtime = "";
        	
        	
        	Double lastmoney = 0.0;
        	Double curmoney = 0.0;
        	
        	String cardinfo = "";

      	    DecimalFormat df = new DecimalFormat("######0.0"); 
      	  
      	    long lastTime = System.currentTimeMillis();
        	while(iter.hasNext())
            {
        		final long currtime = System.currentTimeMillis();
                if (currtime - lastTime > 10000) {
                    context.progress();
                    lastTime = currtime;
                }
                
                
        		Double deltatime = 0.0;
        		Double deltamoney = 0.0;
        		Double tempdelta = 0.0;
        		
        		String[] list = iter.next().toString().split("\\s");
        		curmoney = Double.valueOf(list[2]);
                Moneylist.add(curmoney);
        		
      		  String curTS = list[0] + list[1];    //20160309180406
      		  
      	  if(curTS.length()==14){	
      		    curtime = curTS.substring(0, 4) + "-" + curTS.substring(4, 6) + "-" + curTS.substring(6, 8) +
      				  " " + curTS.substring(8, 10) + ":" + curTS.substring(10, 12) + ":" + curTS.substring(12, 14);
      	
      		  //yyyy-MM-dd HH:mm:ss

      		  if(lasttime.equals(""))
      			  deltatime = null;
      		  else
      			  deltatime = getDeltaMin(curtime,lasttime);

      		  
      		
      		  if(lastmoney==0.0)
      			  deltamoney = null;
      		  else{
      			   tempdelta = curmoney - lastmoney;
      			 deltamoney = tempdelta>=0? tempdelta:null;
      		  }
      			  
//////////////////////////////////////////////////////////////////////////////////////////////////      		  
              HashMap<String, Double> newinfo = new HashMap<String, Double>();
              newinfo.put("dt", deltatime);
              newinfo.put("dm", deltamoney);
              newinfo.put("cm", curmoney);
              Infolist.add(newinfo);
              
      		  lasttime = curtime;
      		  lastmoney = curmoney; 
      	  }
  
            }
        	   
        	  Double moneyAvg = getAve(Moneylist);
        	  int count = Moneylist.size();
        	  Double moneySum = moneyAvg*count;
        
        	   
        	  cardinfo = cardinfo + "moneySum: " + df.format(moneySum) + " moneyAvg: " +  df.format(moneyAvg) + " allCount: " + count + "\n";
        	  
        	  
        	  for(HashMap<String, Double> info :Infolist){
        		  String formatDT = info.get("dt") != null? df.format(info.get("dt")/1):null;
        		  
        		  String tempInfo = formatDT + "\t" + info.get("dm") + "\t" + info.get("cm") + "\t" + df.format(info.get("cm")/moneyAvg);
        		  cardinfo = cardinfo + tempInfo + "\n";
        	  }
 
             result.set(cardinfo);
             context.write(key, result);
             
        }
    }

    public static void main(String[] args) throws Exception {
    	  Configuration conf = new Configuration();
    	  long milliSeconds = 1000*60*60;
    	  conf.setLong("mapreduce.task.timeout", milliSeconds);
    	  
    	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	  if (otherArgs.length != 2) {
    	    System.err.println("Usage: WordCount <in> <out>");
    	    System.exit(2);
    	  }
    	  Job job = new Job(conf, "cardDelta");
    	 
    	  job.setJarByClass(TransDelta.class);
    	  job.setMapperClass(CardMapper.class);
    	  //job.setCombinerClass(DeltaReducer.class);
    	  job.setReducerClass(DeltaReducer.class);
    	  job.setOutputKeyClass(Text.class);
    	  job.setOutputValueClass(Text.class);
    	  job.setNumReduceTasks(300);
     
    	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    	  System.exit(job.waitForCompletion(true) ? 0 : 1);
    	}
    
    
    
    
    public static Double getAve(ArrayList<Double> array) {
		Double ave = 0.0;
		Double sum = 0.0;
		for(Double money: array) {
			sum +=  money;
		}
		ave  = sum/array.size();
		return ave;
	}
    

}