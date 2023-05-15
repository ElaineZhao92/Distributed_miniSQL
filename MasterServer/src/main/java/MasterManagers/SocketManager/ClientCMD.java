package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.net.Socket;

public class ClientCMD {
    private TableManager tableManager;
    private Socket socket;
    public ClientCMD(TableManager tableManager, Socket socket){
        this.tableManager = tableManager;
        this.socket = socket;
    }

    public String processClientCommand(String cmd){
        String result = "";
        String tableName = "";
        System.out.println(cmd);
        if (cmd.startsWith("query")) { // query table_name
            tableName = cmd.substring(6); // 空格
            result = "query "+tableManager.getInetAddress(tableName) +" "+ tableName;
            System.out.println(result);
        } else if (cmd.startsWith("create")) { // create table_name
            tableName = cmd.substring(7);
            result = "create "+tableManager.getIdealServer() + " " +tableName;
            System.out.println(result);
        }
        return result;
    }

}
