package ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

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
            if (line.contains("-->")){System.out.println("CLIENT>>>Info from master is: " + line);}
            else{
                String prompt="CLIENT>>>Info from master is: ";
                System.out.printf(prompt);
                for (int i=0;i<line.length();i++){
                    System.out.printf("-");}
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                    System.out.printf("| ");  
                for(int i=0;i<line.length()-2;i++){
                    if(i%2!=0){System.out.printf("*");}  
                    else{System.out.printf(" ");}
                }
                System.out.printf("|");
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                for (int i=0;i<line.length();i++){
                    System.out.printf("-");}
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                System.out.println(line);
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
