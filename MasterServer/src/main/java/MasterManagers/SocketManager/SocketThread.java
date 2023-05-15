package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import io.netty.util.internal.SocketUtils;
import javafx.scene.layout.Region;
import lombok.extern.slf4j.Slf4j;
import java.net.Socket;

/**
 * 客户端线程，和客户端通信
 */
@Slf4j
public class SocketThread implements Runnable{
    private boolean is_Running = false;
    private Socket socket;
    private TableManager tableManager;
    public BufferedReader input = null;
    public PrintWriter output = null;
    private ClientCMD clientCMD;
    private RegionCMD regionCMD;

    public SocketThread(Socket socket, TableManager tableManager) throws IOException{
        this.tableManager = tableManager;
        this.socket = socket;
        this.is_Running = true;
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);
        this.clientCMD = new ClientCMD(tableManager, socket);
        this.regionCMD = new RegionCMD(tableManager, socket);

        System.out.println("服务端建立新的客户端线程:" + socket.getInetAddress() +":"+ socket.getPort());
    }

    public void run() {
        String line;
        try{
            while(is_Running){
                Thread.sleep(Long.parseLong("1000"));
                // 为什么不是 Thread.sleep(1000)?
                line = input.readLine();
                if(line != null)
                    this.processCmd(line);
            }
        } catch ( InterruptedException | IOException e){
            e.printStackTrace();
        }
    }

    public void processCmd(String cmd){
        log.warn(cmd);
        String result = "";
        if(cmd.startsWith("[client]"))
            result = clientCMD.processClientCommand(cmd.substring(9));
        else if(cmd.startsWith("[region]"))
            result = regionCMD.processRegionCommand(cmd.substring(9));   // 如果这里没有空格 那么就8

        if(!result.equals(""))
            this.sendToRegion(result);
    }

    public void sendToRegion(String result){
        output.println("[master] "+result);
    }

}
