package MasterManager.SocketManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import io.netty.util.internal.SocketUtils;
import javafx.scene.layout.Region;
import lombok.extern.slf4j.Slf4j;
import java.net.Socket;

import MasterManager.TableManager;

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

    public SocketThread(Socket socket, TableManager tableManager) throws IOException{
        this.tableManager = tableManager;
        this.socket = socket;
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
        if(cmd.startsWith("[client]"))
            result = clientProcessCmd(cmd.substring(9));
        else if(cmd.startsWith("[region]"))
            result = regionProcessCmd(cmd.substring(9));   // 如果这里没有空格 那么就8

        if(!result.equals(""))
            this.sendToRegion(result);
    }

    public void sendToRegion(String result){
        output.println("[master]"+result);
    }

    public String clientProcessCmd(String cmd){
        String result = "";
        String tableName = "";
        if (cmd.startsWith("query")) { // query table_name
            tableName = cmd.substring(6); // 空格
            result = "query "+tableManager.getInetAddress(tableName) +" "+ tableName;
        } else if (cmd.startsWith("create")) { // create table_name
            tableName = cmd.substring(7);
            result = "create "+tableManager.getIdealServer() + " " +tableName;
        }
        return result;
    }
    public String regionProcessCmd(String cmd){
        String result = "";
        String ip = socket.getInetAddress().getHostAddress();
        if(ip.equals("127.0.0.1"))
            ip = SocketUtils.loopbackAddress().getHostAddress();
        if (cmd.startsWith("query") && !tableManager.hasServer(ip)) {
            tableManager.addServer(ip);
            String[] allTable = cmd.substring(6).split(" ");
            for(String temp : allTable) {
                tableManager.addTable(temp, ip);
            }
        } else if (cmd.startsWith("add")) {
            // [region] add/delete table_name 情况
            String tableName = cmd.substring(4);
            tableManager.addTable(tableName,ip);
            result += "add Table" + tableName + "\n";

        } else if (cmd.startsWith("delete")) {
            String tableName = cmd.substring(7);
            tableManager.deleteTable(tableName,ip);
            result += "delete Table" + tableName + "\n";

        } else if (cmd.startsWith("drop")){
            log.warn("完成从节点的数据转移");

        } else if (cmd.startsWith("recover")){
            log.warn("完成从节点的恢复，重新上线");

        }

        return result;
    }
}
