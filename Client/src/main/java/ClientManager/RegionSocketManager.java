package ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import org.apache.commons.lang3.StringUtils;
public class RegionSocketManager {
    public Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private String region;

    public RegionSocketManager() {

    }

    public void setRegionIP(String ip) {
        this.region = ip;
    }

    // 与Region建立连接
    // public void connectRegionServer(int PORT) throws IOException {
    //     socket = new Socket(region, PORT);
    //     input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    //     output = new PrintWriter(socket.getOutputStream(), true);
    //     isRunning = true;
    //     this.listenToRegion();
    //     System.out.println("CLIENT>>>connect to region  "+this.region+" : " + PORT);
    // }

    public void connectRegionServer(String ip) throws IOException {
        // System.out.println("connectRegionServer : "+ip);
        socket = new Socket(ip, 22222);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
        this.listenToRegion();
        System.out.println("CLIENT>>>connect to region  " + ip + " : 22222");
    }


    public void receiveFromRegion() throws IOException {
        String line = new String("");
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("CLIENT>>>Socket closed !");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            //print result
            String prompt="CLIENT>>>Info from region is: ";
            int width=10;//每个字段值10个空间
        // if(line!=NULL){
        if(line.contains("|")){
            String []values=line.split("\\|");
            System.out.println(prompt);
            for (int i=1;i<values.length;i++){
                // values[i]=values[i].substring(1);
                System.out.printf("|%s",StringUtils.center(values[i],width));
            }
            System.out.printf("|%n");
        }
        else{
            System.out.println(prompt+line);
        }
        }
    }

    public void listenToRegion() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    public void closeRegionSocket() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.stop();
    }

    // 像从服务器发送信息的api
    public void sendToRegion(String info) {
//        System.out.println("发送给从节点的消息是：" + info);
        output.println(info);
    }


    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println("CLIENT>>>start listening to region!");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromRegion();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    sleep(100);
                } catch (InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
