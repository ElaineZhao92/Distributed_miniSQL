package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.net.Socket;
import java.util.List;

public class ClientCMD {
    private TableManager tableManager;
    private Socket socket;
    public ClientCMD(TableManager tableManager, Socket socket){
        this.tableManager = tableManager;
        this.socket = socket;
    }

    public String processClientCommand(String cmd){
//        System.out.println("收到Client发来的信息 " + cmd);
        String result = "";
        String tableName = "";
        /**
         *         client：发来create       master 找2个目前负载最小的region，分别转发语句，但不是马上addTable
         *         client：发来insert	   master 转发给2个region
         *         client：发来select	   master 转发给 主 region
         *         client：发来delete	   master 转发给2个region
         *         client：发来drop  	   master 转发给2个region
         */

        if (cmd.startsWith("create")) { // create table_name

            tableName = cmd.substring(7);
            // 2个 region，目前负载最小的
            List<String> ips = tableManager.getIdealServer();
            result = "create " + ips.get(0) + " "+ ips.get(1) +" "+ tableName;
            System.out.println(result);

        } else if (cmd.startsWith("insert")) { // create table_name

            tableName = cmd.substring(7); // 空格
            System.out.println("tableName::" + tableName);
            List<String> ips = tableManager.getInetAddress(tableName);
            result = "insert " + ips.get(0) + " "+ ips.get(1) +" "+ tableName;
            System.out.println(result);
        }
        else if (cmd.startsWith("select")) { // create table_name
            tableName = cmd.substring(7);
            List<String> ips = tableManager.getInetAddress(tableName);
            result = "select " + ips.get(0) + " "+ ips.get(1) +" "+ tableName;
            System.out.println(result);
        }
        else if (cmd.startsWith("delete")) { // create table_name
            tableName = cmd.substring(7);
            List<String> ips = tableManager.getInetAddress(tableName);
            result = "delete " + ips.get(0) + " "+ ips.get(1) +" "+ tableName;
            System.out.println(result);
        }
        else if (cmd.startsWith("drop")) { // create table_name
            tableName = cmd.substring(5);
            List<String> ips = tableManager.getInetAddress(tableName);
            result = "drop " + ips.get(0) + " "+ ips.get(1) +" "+ tableName;
            System.out.println(result);
        }
        return result;
    }

}
