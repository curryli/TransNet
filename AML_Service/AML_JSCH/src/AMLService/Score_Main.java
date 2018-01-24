package AMLService;
//java -jar Score_Main.jar input_card.csv "20170603" "20170603"

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import Utils.DeleteFileUtil;
import Utils.JSchUtils;

public class Score_Main {
     
    public static void main(String[] args) throws SftpException, FileNotFoundException {
    	long start=System.currentTimeMillis(); //获取开始时间
    	String input_card =  args[0];
    	String startdate =  args[1];
    	String enddate =  args[2];

    	SimpleDateFormat df = new SimpleDateFormat("MM-dd-HH-mm");//设置日期格式
    	String cur_time = df.format(new Date());
    	String save_Score = "output/Score_" + cur_time + ".csv";
    	
    	try {
        	System.out.println("Starting...");
        	
        	DeleteFileUtil.delete("output_Score.csv");
        	System.out.println("确保执行算法之前,结果文件不存在.");
        	
        	// 连接到指定的服务器
        	JSchUtils.connect("hdrisk", "zaq@1808", "172.18.160.50", 22);  //由于session是static，所以第一次调用connect函数的时候就初始化了一个session,后面所有JSchUtils函数都共享这个session
 
            // 执行相关的命令
        	JSchUtils.exec2String("rm -rf /home/hdrisk/input_card/input_card.csv");
            // 上传原始卡号
        	JSchUtils.upload("/home/hdrisk/input_card", input_card);
  
        	System.out.println("开始执行评分模型..."); 
            String shell_cmd = "sh /home/hdrisk/sh/Score_with_Params.sh " + startdate + " " + enddate;
            JSchUtils.exec2String(shell_cmd);
             
            // 下载原始卡号评分
            JSchUtils.download("/home/hdrisk/output_Score/output_Score.csv", "output_Score.csv");
            JSchUtils.download("/home/hdrisk/output_Score/output_Score.csv", save_Score);
            
            System.out.println("快速浏览：当前文件夹下的 " + "output_Score.csv");
            System.out.println("结果永久保存在：output文件夹下的" + save_Score);		
            System.out.println("程序运行成功！");  
            long end=System.currentTimeMillis(); //获取结束时间  
            System.out.println("程序运行时间： "+(end-start)/(1000 * 60)+"分钟"); 
  
        }catch (JSchException e){
            e.printStackTrace();
            System.out.println("服务器连接出错！！！");
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("程序执行出错！！！");
        }
    	finally{  
          // 关闭连接
          JSchUtils.close();
        }  
 
    }
}