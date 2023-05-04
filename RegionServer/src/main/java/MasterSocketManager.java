

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
    public final String MASTER = "localhost";

    public MasterSocketManager() throws IOException {
        this.socket = new Socket(MASTER, SERVER_PORT);
        this.ftpUtils = new FtpUtils();
        this.dataBaseManager = new DatabaseManager();
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        isRunning = true;
    }

    public void sendToMaster(String modified_info) {
        output.println(modified_info);
    }
    // ??????不在报告里，不知道在干啥，好像是RegionManager测试的时候用的
    public void sendTableInfoToMaster(String table_info) {
        output.println("<region>[1] " + table_info);
    }

    public void receiveFromMaster() throws IOException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("新消息>>>Socket已经关闭!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            if (line.startsWith("<master>[3]")) {
                // <master>[3] drop ip name name
                String info = line.substring(17);
                if(line.length() == 17) return;
                String[] ini_tables = info.split(" ");
                String[] tables = new String[2];
                System.arraycopy(ini_tables, 2, tables, 0, 2);
                // <master[3]>ip#name@name@...
                for(String table : tables) {
                    delFile(table);
                    delFile(table + "_index.index");
                    ftpUtils.downLoadFile("table", table, "");
                    System.out.println("Download successfully: " + table);
                    ftpUtils.downLoadFile("index", table + "_index.index", "");
                    System.out.println("Download successfully: " + table + "_index.index");
                }
                String ip = ini_tables[1];
                ftpUtils.additionalDownloadFile("catalog", ip + "#table_catalog");
                ftpUtils.additionalDownloadFile("catalog", ip + "#index_catalog");
                try {
                    API.initial();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("here");
                output.println("<region>[3] drop successfully");
            }
            else if (line.equals("<master>[4]recover")) {
                String tableName = dataBaseManager.getMetaInfo();
                String[] tableNames = tableName.split(" ");
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
                output.println("<master>[4] recover successfully");
            }
        }
    }

    public void delFile(String fileName) {
        File file = new File(fileName);
        if (file.exists() && file.isFile()) file.delete();
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
}
