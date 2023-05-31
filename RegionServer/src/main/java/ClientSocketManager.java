

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
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

    private boolean isRunning = true;
    private FtpUtils ftpUtils;

    Client(Socket socket,MasterSocketManager masterSocketManager) throws IOException{
        this.socket=socket;
        this.masterSocketManager=masterSocketManager;
        this.ftpUtils=new FtpUtils();
        this.input=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output=new PrintWriter(socket.getOutputStream(),true);
        System.out.println("REGION> 建立了新的客户端子线程：" + socket.getPort());
    }

    //不断循环处理sql语句
    public void run(){
        System.out.println("REGION> 监听客户端消息中" + socket.getInetAddress() + ":" + socket.getPort());

        while(isRunning){
            if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                Thread.sleep(Long.parseLong("1000"));
                String sql = input.readLine();
                if(sql!=null&&!sql.equals("2")){
                    StringBuffer res = new StringBuffer(""); //sql语句处理后的结果
                    boolean isTableModified=getResult(sql,socket.getInetAddress().toString(),res);
                    //System.out.println("getResult ok!!");
                    //System.out.println("res = " + res);
                    //看是否发生了表的增删，需要通知主节点
                    String ress = new String(res);
                    if(isTableModified){
                        masterSocketManager.sendToMaster(ress);
                    }
                }
            } catch (SocketException e) {
                isRunning = false;
//                    e.printStackTrace();

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
        System.out.println("REGION> 要处理的命令：" + sql);
        //处理sql语句
        String result=Interpreter.interpret(sql);
        //保存catalog到文件中
        API.store();
        //返回结果给客户端
        output.println(result);
        String[] sqls=sql.split(" ");
        String[] results=result.split(" ");
        String keyword=sqls[0];
       String tablename = ""; //除了select语句，所有表名都在第三个String
        if(keyword.equals("create")){ //建表
            res.append("[region] create "+ results[2]);
            tablename = results[2];
            flag=true;
        }
        else if(keyword.equals("drop")){ //删表
            res.append("[region] drop "+ results[2]);

            tablename = results[2];
            flag=true;
        }
        else if(keyword.equals("insert")||keyword.equals("delete")){ //记录的增删
            //System.out.println(sqls[2]);
            tablename = sqls[2];
            flag = false;
        }
        else if(keyword.equals("select")){ //记录的增删
            return false;
        }
        System.out.println("REGION> 开始存储sql语句:" + sql);
        File file = new File(tablename + ".txt");
        if(!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Writer writer = new FileWriter(file, true);
        try {
            writer.write(sql + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
        }

        System.out.println("REGION> 存储sql语句成功！");
        return flag;
    }
}

