

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.RoundingMode;
import java.net.Socket;
import java.text.DecimalFormat;
import miniSQL.API;
import miniSQL.Interpreter;
import java.io.FileReader;
public class RegionSocketReceiveManager implements Runnable {
    public String table_name;
    private ServerSocket region_socket;
    private boolean isRunning = false;
    public final int REGION_SERVER_PORT = 1117;

    public FileOutputStream foStream;

//    public DatabaseManager databaseManager;
    public DataInputStream diStream;

    public DecimalFormat dFormat;

    RegionSocketReceiveManager(int port) throws IOException{
//        this.databaseManager = databaseManager;
        this.region_socket=new ServerSocket(REGION_SERVER_PORT);
        this.isRunning = true;
        dFormat = new DecimalFormat("#0.0");
        dFormat.setRoundingMode(RoundingMode.HALF_UP);
        dFormat.setMinimumFractionDigits(1);
        dFormat.setMaximumFractionDigits(1);
        System.out.println("REGION>从节点接收线程已启动！！PORT：" + REGION_SERVER_PORT);
    }
    //不断循环连接从节点
    public void run() {
        String line;
        while(true){
            try {
                Socket new_region = region_socket.accept();
                Thread HandlerTHread = new Thread(new RegionSocketReceiveHandler(new_region, table_name));
                HandlerTHread.start();
            } catch (Exception e) {
                System.out.println("REGION>Region copy 接收异常：" + e.getMessage());
            }
        }
    }

    private class RegionSocketReceiveHandler implements Runnable {
        private Socket socket;


        public BufferedReader input = null;
        public PrintWriter output = null;
        public String table_name;

        public RegionSocketReceiveHandler(Socket socket, String table_name) throws IOException{
            this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.output = new PrintWriter(socket.getOutputStream(), true);
            this.socket = socket;
            this.table_name = table_name;
        }
        public void run() {
            System.out.println("REGION>本节点已被从节点连接！");
            String line;
            try {
                System.out.println("REGION> 收到文件::" + table_name);
                get();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        private String getFormatFileSize(long length) {
            double size = ((double) length) / (1 << 30);
            if (size >= 1) {
                return dFormat.format(size) + "GB";
            }
            size = ((double) length) / (1 << 20);
            if (size >= 1) {
                return dFormat.format(size) + "MB";
            }
            size = ((double) length) / (1 << 10);
            if (size >= 1) {
                return dFormat.format(size) + "KB";
            }
            return length + "B";
        }

        public void get() {
            try {
                diStream = new DataInputStream(socket.getInputStream());
                //文件名和长度
                String fileName = diStream.readUTF();
                long fileLength = diStream.readLong();
                File directory = new File( ".");
                if (!directory.exists()) {
                    directory.mkdir();
                }
                File file = new File(directory.getAbsolutePath() + File.separatorChar + fileName);
                foStream = new FileOutputStream(file);

                //开始接收文件
                byte[] bytes = new byte[1024];
                int length = 0;
                while ((length = diStream.read(bytes, 0, bytes.length)) != -1) {
                    foStream.write(bytes, 0, length);
                    foStream.flush();
                }
                System.out.println("REGION>收到文件[ File Name: " + fileName + " ] [ Size: " + getFormatFileSize(fileLength) + " ]");
                //开始读取文件
                System.out.println("REGION>文件内容如下：");
                BufferedReader reader;
                try {
                    reader = new BufferedReader(new FileReader(
                            fileName));
                    String line = reader.readLine();
                    while (line != null) {
                        System.out.println("REGION>" + line);
                        Interpreter.interpret(line);
                        System.out.println("REGION>执行完毕！");
                        // read next line
                        line = reader.readLine();
                    }
                    reader.close();
                    API.store();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (foStream != null) {
                        foStream.close();
                    }
                    if (diStream != null) {
                        diStream.close();
                    }
                    socket.close();
                } catch (Exception e2) {
                    e2.printStackTrace();
                }
            }// end try
        }// end get
    }
}

