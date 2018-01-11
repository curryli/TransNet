package Utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
 
import java.io.IOException;
import java.io.InputStream;
  
public class test_simpleJSCH {
    public static void main(String[] args) throws JSchException, IOException {
 
        String command = "sh /home/hdanaly/xrli/TeleTrans/testsh.sh";
 
        JSch jsch = new JSch();
        Session session = jsch.getSession("hdanaly", "172.18.160.50", 22);
        session.setPassword("hdanaly");
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(60 * 1000);
        Channel channel = session.openChannel("exec");
        ((ChannelExec) channel).setCommand(command);
 
        channel.setInputStream(null);
 
        ((ChannelExec) channel).setErrStream(System.err);
 
        InputStream in = channel.getInputStream();
 
        channel.connect();
 
        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                System.out.print(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                if (in.available() > 0) continue;
                System.out.println("exit-status: " + channel.getExitStatus());
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception ee) {
            }
        }
        channel.disconnect();
        session.disconnect();
 
    }
}