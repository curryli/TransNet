package AMLService;
//java -jar Score_Main.jar input_card.csv "20170603" "20170603"

import java.io.FileNotFoundException;
  
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
 
import Utils.JSchUtils;

public class KillSpark {
     
    public static void main(String[] args) throws SftpException, FileNotFoundException {
    	
    	try {
        	System.out.println("Starting...");
        	// 连接到指定的服务器
        	JSchUtils.connect("hdrisk", "zaq@1808", "172.18.160.50", 22);  //由于session是static，所以第一次调用connect函数的时候就初始化了一个session,后面所有JSchUtils函数都共享这个session

            // 执行相关的命令
            String yarn_rt = JSchUtils.exec2String("yarn application -list -queue root.queue_hdrisk -appTypes SPARK");
           
            String[] arr = yarn_rt.split("\\s+");  //.getClass().toString());
            
            String AppID = null;
            
            for(int i=0; i<arr.length; i++){
              if(arr[i].contains("queue_hdrisk") && arr[i-3].contains("ScoreMd5Cards")){
            	  AppID =arr[i-4].substring(12);
              }
            }
            System.out.println("强制结束Spark进程." + AppID); 
            String kill_CMD = "yarn application -kill " + AppID;
            JSchUtils.exec2String(kill_CMD);
             
            System.out.println("完成."); 
             
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