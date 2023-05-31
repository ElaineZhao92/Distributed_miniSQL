import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


// 负责和主节点进行通信的类
public class RegionSocketSendManager implements Runnable {

    private FileInputStream fileIn;
    private DataOutputStream DataOUT;

    private File file;
    private Socket region_socket;
    // private FtpUtils ftpUtils;
    private boolean isRunning = false;
    private PrintWriter master_output = null;

    public final int SERVER_PORT = 1117;
    public String DES_REGION;
    String table_name;

    public RegionSocketSendManager(String DES_REGION, String table_name, PrintWriter master_output) throws IOException {
        this.region_socket = new Socket(DES_REGION, SERVER_PORT);
        System.out.println("region: " + DES_REGION + ", port:" + SERVER_PORT);
        this.DES_REGION = DES_REGION;
        this.table_name = table_name;
        this.master_output = master_output;
        isRunning = true;
    }

    @Override
    public void run() {
        System.out.println("REGION> 从节点之间发送副本线程启动！");
        System.out.println("REGION> 从节点之间发送副本成功！");
        while (isRunning) {
            if (region_socket.isClosed() || region_socket.isInputShutdown() || region_socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                  sendFile(new File(table_name));

            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }
    private void sendFile(File file) throws Exception {
        try {

            if (file.exists()) {
                fileIn = new FileInputStream(file);
                DataOUT = new DataOutputStream(region_socket.getOutputStream());

                //文件名和长度
                DataOUT.writeUTF(file.getName());
                DataOUT.flush();
                DataOUT.writeLong(file.length());
                DataOUT.flush();

                //开始传输文件
                System.out.println("=========Start to transfer=========");
                byte[] bytes = new byte[1024];
                int length = 0;
                long progress = 0;
                while ((length = fileIn.read(bytes, 0, bytes.length)) != -1) {
                    DataOUT.write(bytes, 0, length);
                    DataOUT.flush();
                    progress += length;
                    System.out.println("| " + (100 * progress / file.length()) + "% |");
                }
                System.out.println();
                System.out.println("=====File transferred successfully=====");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {                                //关闭数据流
            if (fileIn != null) {
                fileIn.close();
            }
            if (DataOUT != null) {
                DataOUT.close();
            }
        }
    }
}
