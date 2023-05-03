package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.net.Socket;

public class ClientProcessor {
    private TableManager tableManager;
    private Socket socket;

    public ClientProcessor(Socket socket, TableManager tableManager){
        this.socket = socket;
        this.tableManager = tableManager;
    }

    public String processCmd(String cmd){
        String result = "";
        String tableName = cmd.substring(3); // 去掉头上的[1]
        if(cmd.startsWith("[1]"))
            result = "[1]"+tableManager.getInetAddress(tableName) +" "+ tableName;
        else if(cmd.startsWith("[2]"))
            result = "[2]"+tableManager.getInetAddress(tableName) +" "+ tableName;

        return result;
    }
}
