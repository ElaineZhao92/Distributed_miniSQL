package ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

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


    public boolean connectRegionServer(String ip) throws IOException ,ConnectException{
        // System.out.println("connectRegionServer : "+ip);
    try{ 
        socket = new Socket(ip, 22222);
        socket.sendUrgentData(0xFF);
    }
    catch(ConnectException se){ 
        return false; 
    } 
    catch (Exception e) {
        e.printStackTrace();
    }     
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
        this.listenToRegion();
        System.out.println("CLIENT>connect to region  " + ip + " : 22222");
        return true;
    }


    public void receiveFromRegion() throws IOException {
        String line = new String("");
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("CLIENT>Socket closed !");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            //print result
            String prompt="";
            int width=10;//每个字段值10个空间
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
            if(line.equals("-----")) {System.out.println(prompt);}
            else{System.out.println(prompt+line);
            }
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
            System.out.println("CLIENT>start listening to region!");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromRegion();
                } catch (SocketException e) {
                    isRunning = false;
//                    e.printStackTrace();

                } catch (IOException e){
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
