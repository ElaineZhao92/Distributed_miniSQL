package ClientManager;

// import ClientManager.ClientManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
public class MasterSocketManager {

    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private boolean isRunning = false;
    private Thread infoListener;

    private ClientManager clientManager;

    // 服务器的IP和端口号
    private final String master = "192.168.43.103";
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
    public void sendToMaster(String op, String table) {
        output.println("[client] " + op+" "+ table);
    }

    public void sendToMasterCreate(String op, String table) {
        output.println("[client] " + op+" "+ table);
    }

    // 接收来自master server的信息并显示
    // 新增代码，查询主服务器中存储的表名和对应的端口号
    // 主服务器返回的内容的格式应该是"<table>table port"，因此args[0]和[1]分别代表了表名和对应的端口号
    public void receiveFromMaster() throws IOException, InterruptedException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("CLIENT>Socket closed !");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            //print result
            String prompt="CLIENT>Info from master is: ";
            int width=10;//每个字段值10个空间
        // if(line!=NULL){
        if(line.contains("|")){
            String []values=line.split("\\|");
            for (int i=1;i<values.length;i++){
                // values[i]=values[i].substring(1);
                System.out.printf("|%s",StringUtils.center(values[i],width));
            }
            System.out.printf("|%n");
        }
        else{
            System.out.println(prompt+line);
        }
            
            // 主节点通信协议的解析方案
            if (line.startsWith("[master]")) {
                // 截取ip地址
                String[] args = line.split(" ");
                String op= args[1];
                if(op.equals("show")){
                    System.out.printf("CLIENT>");
                    for (int i=2;i<args.length;i++){
                        System.out.printf(args[i]);
                    }
                    System.out.println("");
                    return ;
                }
                System.out.println(args[0] + "|" + args[1] + "|" + args[2] + "|" + args[3]);
                String fip = args[2], sip=args[3],table = args[4];
                this.clientManager.cacheManager.setfCache(table, fip);
                this.clientManager.cacheManager.setsCache(table, sip);
                //发送给主 副两个节点
                
                this.clientManager.connectToRegion(fip, commandMap.get(table));
                if(!op.equals("select") && (!sip.equals(fip))){
                    this.clientManager.connectToRegion(sip, commandMap.get(table));}
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

    public void process(String sql,String op, String table) {
        // 来处理sql语句
        if(op.equals("show")){table="tables";}
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        this.sendToMaster(op,table);
    }

    public void processCreate(String sql,String op, String table) {
        this.commandMap.put(table, sql);
        // 用<table>前缀表示要查某个表名对应的端口号
        System.out.println( " create the table : " + table);
        this.sendToMasterCreate(op,table);
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
            System.out.println("CLIENT>start listening to master!");
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
