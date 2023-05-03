package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javafx.scene.layout.Region;
import lombok.extern.slf4j.Slf4j;
import java.net.Socket;

/**
 * 客户端线程，和客户端通信
 */
@Slf4j
public class SocketThread implements Runnable{

    private boolean is_Running = false;
    private ClientProcessor clientProcessor;
    private RegionProcessor regionProcessor;
    public BufferedReader input = null;
    public PrintWriter output = null;

    public SocketThread(Socket socket, TableManager tableManager) throws IOException{
        this.clientProcessor = new ClientProcessor(socket, tableManager);
        this.regionProcessor = new RegionProcessor(socket, tableManager);
        this.is_Running = true;
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);

        System.out.println("服务端建立新的客户端线程:" + socket.getInetAddress() +":"+ socket.getPort());
    }

    public void run() {
        String line;
        try{
            while(is_Running){
                Thread.sleep(Long.parseLong("1000"));
                // 为什么不是 Thread.sleep(1000)?
                line = input.readLine();
                if(line != null)    this.processCmd(line);
            }
        } catch ( InterruptedException | IOException e){
            e.printStackTrace();
        }
    }

    public void processCmd(String cmd){
        log.warn(cmd);
        String result = "";
        if(cmd.startsWith("<client>"))
            result = clientProcessor.processCmd(cmd.substring(8));
        else if(cmd.startsWith("<region>"))
            result = regionProcessor.processCmd(cmd.substring(8));

        if(!result.equals(""))
            this.sendToRegion(result);
    }

    public void sendToRegion(String result){
        output.println("<master>"+result);
    }
}
