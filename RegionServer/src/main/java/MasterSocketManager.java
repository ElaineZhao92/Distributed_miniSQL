

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

import lombok.SneakyThrows;
import miniSQL.API;
import miniSQL.Interpreter;

// 负责和主节点进行通信的类
public class MasterSocketManager implements Runnable {
    private Socket socket;
    private BufferedReader input = null;
    private PrintWriter output = null;
    private FtpUtils ftpUtils;
    private DatabaseManager dataBaseManager;
    private boolean isRunning = false;

    public final int SERVER_PORT = 12345;
    public final String MASTER = "192.168.155.135";

    public MasterSocketManager() throws IOException {
        this.socket = new Socket(MASTER, SERVER_PORT);
        this.ftpUtils = new FtpUtils();
        this.dataBaseManager = new DatabaseManager();
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
    }

    @Override
    public void run() {
        System.out.println("新消息>>>从节点的主服务器监听线程启动！");
        while (isRunning) {
            if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                isRunning = false;
                break;
            }
            try {
                receiveFromMaster();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException | NullPointerException e) {
                e.printStackTrace();
            }
        }
    }

    public void sendToMaster(String modified_info) {
        output.println(modified_info);
    }
    
    public void sendTableInfoToMaster(String table_info) {
        output.println("[region] recover " + table_info);
    }

    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("receive from Master>> line::" + line);
            if (line.startsWith("[master] drop ")) {
                System.out.println("master::drop");
                String info = line.substring(14);
                if(line.length()==14) return;
                // [master] drop ip name name ...
                String[] tables = info.split(" ");
                int i = 0;
                for(String table : tables) {
                    if (i < 2) continue;
                    delFile(table);
                    delFile(table + "_index.index");
                    ftpUtils.downLoadFile("table", table, "");
                    System.out.println("success " + table);
                    ftpUtils.downLoadFile("index", table + "_index.index", "");
                    System.out.println("success " + table + "_index.index");
                    i++;
                }
                // String ip = info.split("#")[0];
                String ip = tables[2];
                ftpUtils.additionalDownloadFile("catalog", ip + "#table_catalog");
                ftpUtils.additionalDownloadFile("catalog", ip + "#index_catalog");
                try {
                    API.initial();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("here");
                output.println("[region] drop successfully");
            }
            else if (line.startsWith("[master] recover")) {
                System.out.println("master::recover");
                String tableName = dataBaseManager.getMetaInfo();
                String[] tableNames = tableName.split(" ");
                if(tableNames.length != 0) {
                    for(String table: tableNames) {
                        Interpreter.interpret("drop table " + table + " ;");
                        try {
                            API.store();
                            API.initial();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                output.println("[region] recover successfully");
            }
            else if (line.startsWith("[master] copy")) {
                System.out.println("master::copy");
                String[] info = line.split(" ");
                System.out.println("ip::" + info[2]);
                System.out.println("table_name::" + info[3]);
                Thread RegionSocketSendThread = new Thread(new RegionSocketSendManager(info[2], info[3], output));
                RegionSocketSendThread.start();
            }
        }
    }

    public void delFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile()) file.delete();
    }
}
