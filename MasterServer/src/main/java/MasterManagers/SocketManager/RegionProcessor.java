package MasterManagers.SocketManager;

import MasterManagers.TableManager;
import io.netty.util.internal.SocketUtils;

import java.net.Socket;

public class RegionProcessor {
    private Socket socket;
    private TableManager tableManager;

    public RegionProcessor(Socket socket, TableManager tableManager){
        this.socket = socket;
        this.tableManager = tableManager;
    }

    public String processCmd(String cmd){
        String result = "";
        String ip = socket.getInetAddress().getHostAddress();
        if(ip.equals("127.0.0.1"))
            ip = SocketUtils.loopbackAddress().getHostAddress();
        if(cmd.startsWith("[1]") && !tableManager.hasServer(ip)){
            tableManager.addServer(ip);
            // String [] allTa 没写完！！
        }



        return result;
    }
}
