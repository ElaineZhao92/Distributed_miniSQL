

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import javax.net.ssl.ManagerFactoryParameters;

import MasterManagers.utils.SocketUtils; //这块还没人写
import miniSQL.API;
import miniSQL.Interpreter;

//负责从节点与客户端通信
public class ClientSocketManager implements Runnable{
    private MasterSocketManager masterSocketManager;
    private ServerSocket serversocket;

    ClientSocketManager(int port, MasterSocketManager masterSocketManager) throws IOException{
        this.serversocket=new ServerSocket(port);
        this.masterSocketManager=masterSocketManager;
    }

    //不断循环连接客户端
    public void run(){
        while(true){
            Socket socket;
            try {
                socket = serversocket.accept();
                Client client=new Client(socket,masterSocketManager);
                new Thread(client).start();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
        }
    }

}

class Client implements Runnable{
    private Socket socket;
    private MasterSocketManager masterSocketManager;
    private BufferedReader input;
    private PrintWriter output;
    private FtpUtils ftpUtils;

    Client(Socket socket,MasterSocketManager masterSockerManager) throws IOException{
        this.socket=socket;
        this.masterSocketManager=masterSockerManager;
        this.ftpUtils=new FtpUtils();
        this.input=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output=new PrintWriter(socket.getOutputStream(),true);
    }

    //不断循环处理sql语句
    public void run(){
        while(true){
            String sql;
            try {
                sql = input.readLine();
                if(sql!=null){
                    String res=""; //sql语句处理后的结果
                    boolean isTableModified=getResult(sql,socket.getInetAddress().toString(),res);
                    //看是否发生了表的增删，需要通知主节点
                    if(isTableModified){
                        masterSocketManager.sendToMaster(res);
                    }
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }


        }
    }

    public boolean getResult (String sql,String ip,String res) throws Exception {
        boolean flag=false;
        //处理sql语句
        String result=Interpreter.interpret(sql);
        //保存catalog到文件中
        API.store();
        //返回结果给客户端
        output.println(result);
        //把catalog备份到FTP上
        ftpUtils.uploadFile("table_catalog", SocketUtils.getHostAddress(), "catalog");
        ftpUtils.uploadFile("index_catalog", SocketUtils.getHostAddress(), "catalog");
        
        String[] sqls=sql.split(" ");
        String keyword=sqls[0].toLowerCase();
        String tablename=sqls[2]; //除了select语句，所有表名都在第三个String
        if(keyword.equals("create")){ //建表
            //表名保存到ftp上
            sendToFTP(tablename);
            res="[region] create add "+tablename;
            flag=true;
        }
        else if(keyword.equals("drop")){ //删表
            //把表名从ftp上删除
            deleteFromFTP(tablename);
            res="[region] create delete "+tablename;
            flag=true;
        }
        else if(keyword.equals("insert")||keyword.equals("delete")){ //记录的增删
            //从ftp上删掉旧的表，发送新的表
            deleteFromFTP(tablename);
            sendToFTP(tablename);
        }

        return flag;
    }

    public void sendToFTP(String fileName) { //表名作为文件名
        ftpUtils.uploadFile(fileName, "table");
        ftpUtils.uploadFile(fileName + "_index.index", "index");
    }

    public void deleteFromFTP(String fileName) {
        ftpUtils.deleteFile(fileName, "table");
        ftpUtils.deleteFile(fileName + "_index.index", "index");
    }

}

