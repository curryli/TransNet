package AMLService;
//java -jar Score_Main.jar input_card.csv "20170603" "20170603"
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import Utils.DeleteFileUtil;
import Utils.JSchUtils;

public class Score_Main {
     
    public static void main(String[] args) throws SftpException, FileNotFoundException {
        
    	String input_card =  args[0];
    	String startdate =  args[1];
    	String enddate =  args[2];
    	try {
        	System.out.println("Starting...");
        	
        	System.out.println("确保执行算法之前,结果文件不存在.");
        	DeleteFileUtil.delete("output_Score.csv");
        	
        	// 连接到指定的服务器
        	JSchUtils.connect("hdrisk", "zaq@1808", "172.18.160.50", 22);  //由于session是static，所以第一次调用connect函数的时候就初始化了一个session,后面所有JSchUtils函数都共享这个session
 
            // 执行相关的命令
        	JSchUtils.execCmd("pwd");

        	
            // 上传原始卡号
        	JSchUtils.execCmd("rm -rf /home/hdrisk/input_card/input_card.csv");
        	JSchUtils.upload("/home/hdrisk/input_card", input_card);
 
            
            String shell_cmd = "sh /home/hdrisk/sh/Score_with_Params.sh " + startdate + " " + enddate;
            JSchUtils.execCmd(shell_cmd);
            
 
            // 下载原始卡号评分
            JSchUtils.download("/home/hdrisk/output_Score/output_Score.csv", "output_Score.csv");
             
            // 关闭连接
            JSchUtils.close();
            
            
        } catch (JSchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally{  
          System.out.println("All done."); 
          System.out.println("评分结果存在当前目录下的output_Score.csv");
        }  
 
    }
}