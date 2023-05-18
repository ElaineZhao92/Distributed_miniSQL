package ClientManager;

// import ClientManager.ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MasterSocketManager {

    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private ClientManager clientManager;

    // 服务器的IP和端口号
    private final String master = "10.181.193.235";
    private final int PORT = 12345;

    // 使用map来存储需要处理的表名-sql语句的对应关系
    Map<String, String> commandMap = new HashMap<>();

    public MasterSocketManager(ClientManager clientManager) throws IOException {
        socket = new Socket(master, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;

        this.clientManager = clientManager;

        this.listenToMaster(); // 开启监听线程
    }

    // 像主服务器发送信息的api
    // 要加上client标签，可以被主服务器识别
    public void sendToMaster(String info) {
        output.println("[client] query " + info );
    }

    public void sendToMasterCreate(String info) {
        output.println("[client] create " + info);
    }

    // 接收来自master server的信息并显示
    // 新增代码，查询主服务器中存储的表名和对应的端口号
    // 主服务器返回的内容的格式应该是"<table>table port"，因此args[0]和[1]分别代表了表名和对应的端口号
    public void receiveFromMaster() throws IOException, InterruptedException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("CLIENT>>>Socket closed !");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            if (line.contains("-->")){System.out.println("CLIENT>>>Info from master is: " + line);}
            else{
                String prompt="CLIENT>>>Info from master is: ";
                System.out.printf(prompt);
                for (int i=0;i<line.length();i++){
                    System.out.printf("-");}
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                    System.out.printf("| ");  
                for(int i=0;i<line.length()-2;i++){
                    if(i%2!=0){System.out.printf("*");}  
                    else{System.out.printf(" ");}
                }
                System.out.printf("|");
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                for (int i=0;i<line.length();i++){
                    System.out.printf("-");}
                System.out.printf("\n");
                for (int i=0;i<prompt.length();i++){
                    System.out.printf(" ");}
                System.out.println(line);
            }
            // 已经废弃的方案
            // if (line.startsWith("<table>")) {
            //     String[] args = line.substring(7).split(" ");
            //     String sql = commandMap.get(args[0]);
            //     System.out.println(sql);
            //     // 如果查到的端口号有对应的表
            //     if (sql != null) {
            //         int PORT = Integer.parseInt(args[1]);
            //         commandMap.remove(args[0]);
            //         // 查询到之后在client的cache中设置一个缓存
            //         this.clientManager.cacheManager.setCache(args[0], String.valueOf(PORT));
            //         this.clientManager.connectToRegion(PORT, sql);
            //     }
            // }
            // 主节点通信协议的解析方案
            if (line.startsWith("[master]")) {
                // 截取ip地址
                String[] args = line.split(" ");
                System.out.println(args[0] + "|" + args[1] + "|" + args[2] + "|" + args[3]);
                String ip = args[2], table = args[3];
                this.clientManager.cacheManager.setCache(table, ip);
                this.clientManager.connectToRegion(ip, commandMap.get(table));
            }
        }

    }

    // 开启一个监听线程
    public void listenToMaster() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    // 将sql语句发送到主服务器进一步处理，这里还有待进一步开发，目前仅供实验
    // 进一步开发在这个方法里面扩展

    public void process(String sql, String table) {
        // 来处理sql语句
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        this.sendToMaster(table);
    }

    public void processCreate(String sql, String table) {
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        System.out.println( " create the table : " + table);
        this.sendToMasterCreate(table);
    }

    // 关闭socket的方法，在输入quit的时候直接调用
    public void closeMasterSocket() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        infoListener.stop();
    }


    // 用一个内部类来实现客户端的监听
    // 这里其实参考了Java应用技术里的聊天室的设计
    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println("CLIENT>>>start listening to master!");
            while (isRunning) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    isRunning = false;
                    break;
                }

                try {
                    receiveFromMaster();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    sleep(100);
                } catch (InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}