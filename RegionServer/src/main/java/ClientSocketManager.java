

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
    private HashMap<Socket,Thread> clientHashMap;
    ClientSocketManager(int port, MasterSocketManager masterSocketManager) throws IOException{
        this.serversocket=new ServerSocket(port);
        this.masterSocketManager=masterSocketManager;
        this.clientHashMap=new HashMap<Socket,Thread>();
    }

    //不断循环连接客户端
    public void run(){
        while(true){
            try {
                Thread.sleep(1000);
                Socket socket = serversocket.accept();
                Client client=new Client(socket,masterSocketManager);
                Thread thread=new Thread(client);
                this.clientHashMap.put(socket,thread);
                thread.start();
            } catch (InterruptedException | IOException e) {
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

    Client(Socket socket,MasterSocketManager masterSocketManager) throws IOException{
        this.socket=socket;
        this.masterSocketManager=masterSocketManager;
        this.ftpUtils=new FtpUtils();
        this.input=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output=new PrintWriter(socket.getOutputStream(),true);
        System.out.println("服务端建立了新的客户端子线程：" + socket.getPort());
    }

    //不断循环处理sql语句
    public void run(){
        System.out.println("服务器监听客户端消息中" + socket.getInetAddress() + socket.getPort());

        while(true){
            try {
                Thread.sleep(Long.parseLong("1000"));
                String sql = input.readLine();
                if(sql!=null){
                    StringBuffer res = new StringBuffer(""); //sql语句处理后的结果
                    boolean isTableModified=getResult(sql,socket.getInetAddress().toString(),res);
                    System.out.println("getResult ok!!");
                    System.out.println("res = " + res);
                    //看是否发生了表的增删，需要通知主节点
                    String ress = new String(res);
                    if(isTableModified){
                        masterSocketManager.sendToMaster(ress);
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

    public boolean getResult (String sql,String ip,StringBuffer res) throws Exception {
        boolean flag=false;
        System.out.println("要处理的命令：" + sql);
        //处理sql语句
        String result=Interpreter.interpret(sql);
        //保存catalog到文件中
        API.store();
        //返回结果给客户端
        output.println(result);
        //把catalog备份到FTP上
//        ftpUtils.uploadFile("table_catalog", SocketUtils.getHostAddress(), "catalog");
//        ftpUtils.uploadFile("index_catalog", SocketUtils.getHostAddress(), "catalog");
        
        String[] sqls=sql.split(" ");
        String[] results=result.split(" ");
        String keyword=sqls[0];
        System.out.println("keyword::" + keyword);
        // .toLowerCase();
//      String tablename=results[2]; //除了select语句，所有表名都在第三个String
        if(keyword.equals("create")){ //建表
            //表名保存到ftp上
//            sendToFTP(results[2]);
            System.out.println("res == " + res);
//            res="[region] create add "+ results[2];
            res.append("[region] create "+ results[2]);

            System.out.println("res == " + res);
            flag=true;
            return flag;
        }
        else if(keyword.equals("drop")){ //删表
            //把表名从ftp上删除
//            deleteFromFTP(results[2]);
//            res="[region] create delete "+results[2];
            res.append("[region] drop "+ results[2]);

            flag=true;
            return flag;
        }
        else if(keyword.equals("insert")||keyword.equals("delete")){ //记录的增删
            //从ftp上删掉旧的表，发送新的表
            System.out.println("Insert/Delete::!!");
            System.out.println(sqls[2]);
//            deleteFromFTP(sqls[2]);
//            sendToFTP(sqls[2]);
            System.out.println("success");
            flag = false;
            return flag;
        }

        return false;
    }

    public void sendToFTP(String fileName) { //表名作为文件名
        System.out.println("sendToFTP" + fileName);
        ftpUtils.uploadFile(fileName, "table");
        ftpUtils.uploadFile(fileName + "_index.index", "index");
    }

    public void deleteFromFTP(String fileName) {
        System.out.println("deleteFromFTP" + fileName);
        ftpUtils.deleteFile(fileName, "table");
        ftpUtils.deleteFile(fileName + "_index.index", "index");
    }

}

