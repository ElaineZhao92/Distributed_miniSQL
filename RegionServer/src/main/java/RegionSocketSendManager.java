import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
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
    private Socket region_socket;
    // private FtpUtils ftpUtils;
    private boolean isRunning = false;
    private PrintWriter master_output = null;

    public final int SERVER_PORT = 1117;
    public String DES_REGION;
    String table_name;

    public RegionSocketSendManager(String DES_REGION, String table_name, PrintWriter master_output) throws IOException {
        this.region_socket = new Socket(DES_REGION, SERVER_PORT);
        this.table_name = table_name;
        this.master_output = master_output;
        isRunning = true;
    }

    @Override
    public void run() {
        System.out.println("新消息>>>从节点之间发送副本线程启动！");
        while (isRunning) {
            if (region_socket.isClosed() || region_socket.isInputShutdown() || region_socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                sendToRegionNotice();
                sendToRegion();
                this.table_name = this.table_name + "_index";
                sendToRegion();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }
    public void sendToRegionNotice() throws IOException {
        PrintWriter output = new PrintWriter(region_socket.getOutputStream(), true);
        output.println("[region] copy " + this.table_name);
    }
    public void sendToRegion() throws IOException {
        OutputStream out = region_socket.getOutputStream();
        FileInputStream in = new FileInputStream(new File("../../../../" + table_name));
        byte[] buf = new byte[2048];
        int len;
        while((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
        }
        region_socket.shutdownOutput();
        InputStream sin = region_socket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(sin));
        String line = null;
        while((line = reader.readLine()) != null) {
            master_output.println(line);
        }
        out.close();
        in.close();
        region_socket.close();
        sin.close();
        reader.close();
    }
}
