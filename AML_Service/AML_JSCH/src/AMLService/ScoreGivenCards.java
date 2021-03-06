package AMLService;
//java -jar ScoreGivenCards.jar input_card.csv "20170603" "20170603"
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Vector;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class ScoreGivenCards {
    private static JSch jsch;
    private static Session session;

    /**
     * 连接到指定的IP
     * 
     * @throws JSchException
     */
    public static void connect(String user, String passwd, String host, int port) throws JSchException {
        jsch = new JSch();// 创建JSch对象
        session = jsch.getSession(user, host, port);// 根据用户名、主机ip、端口号获取一个Session对象
        session.setPassword(passwd);// 设置密码

        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);// 为Session对象设置properties
        session.setTimeout(1500);// 设置超时
        session.connect();// 通过Session建立连接
    }

    /**
     * 关闭连接
     */
    public static void close() {
        session.disconnect();
    }

    /**
     * 执行相关的命令
     * 
     * @throws JSchException
     */
    public static void execCmd(String command) throws JSchException {
        BufferedReader reader = null;
        Channel channel = null;
        try {
            if (command != null) {
                channel = session.openChannel("exec");
                ((ChannelExec) channel).setCommand(command);
                // ((ChannelExec) channel).setErrStream(System.err);
                channel.connect();

                InputStream in = channel.getInputStream();
                reader = new BufferedReader(new InputStreamReader(in));
                String buf = null;
                while ((buf = reader.readLine()) != null) {
                    System.out.println(buf);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSchException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            channel.disconnect();
        }
    }

    /**
     * 上传文件
     *
     * @param directory
     *            上传的目录
     * @param uploadFile
     *            要上传的文件
     * @param sftp
     * @throws JSchException
     * @throws SftpException
     * @throws FileNotFoundException
     */
    public static void upload(String directory, String uploadFile) throws JSchException, FileNotFoundException, SftpException {
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.cd(directory);
        File file = new File(uploadFile);
        channelSftp.put(new FileInputStream(file), file.getName());
        System.out.println("Upload Success!");
    }

    /**
     * 下载文件
     * 
     * @param src
     * @param dst
     * @throws JSchException
     * @throws SftpException
     */
    public static void download(String src, String dst) throws JSchException, SftpException {
        // src linux服务器文件地址，dst 本地存放地址
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.connect();
        channelSftp.get(src, dst);
        channelSftp.quit();
    }

    /**
     * 删除文件
     *
     * @param directory
     *            要删除文件所在目录
     * @param deleteFile
     *            要删除的文件
     * @param sftp
     * @throws SftpException
     * @throws JSchException
     */
    public void delete(String directory, String deleteFile) throws SftpException, JSchException {
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        channelSftp.cd(directory);
        channelSftp.rm(deleteFile);
    }

    /**
     * 列出目录下的文件
     *
     * @param directory
     *            要列出的目录
     * @param sftp
     * @return
     * @throws SftpException
     * @throws JSchException
     */
    @SuppressWarnings("rawtypes")
    public Vector listFiles(String directory) throws JSchException, SftpException {
        ChannelSftp channelSftp = (ChannelSftp) session.openChannel("sftp");
        return channelSftp.ls(directory);
    }

    public static void main(String[] args) throws SftpException, FileNotFoundException {
        
    	String input_card =  args[0];
    	String startdate =  args[1];
    	String enddate =  args[2];
    	try {
        	System.out.println("Starting...");
            // 连接到指定的服务器
            connect("hdrisk", "zaq@1808", "172.18.160.50", 22);

            // 上传原始卡号
            execCmd("rm -rf /home/hdrisk/input_card/input_card.csv");
            upload("/home/hdrisk/input_card", input_card);
 
            // 执行相关的命令
            execCmd("pwd");
            
            String shell_cmd = "sh /home/hdrisk/sh/Score_with_Params.sh " + startdate + " " + enddate;
            execCmd(shell_cmd);
            
 
            // 下载原始卡号评分
            download("/home/hdrisk/output_Score/output_Score.csv", "output_Score.csv");
             
            // 关闭连接
            close();
        } catch (JSchException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally{  
          System.out.println("All done.");  
        }  
 
    }
}