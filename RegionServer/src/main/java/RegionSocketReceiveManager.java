

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

public class RegionSocketReceiveManager implements Runnable {
    private ServerSocket region_socket;
    private boolean isRunning = true;
    public final int REGION_SERVER_PORT = 1117;

    RegionSocketReceiveManager(int port) throws IOException{
        this.region_socket=new ServerSocket(REGION_SERVER_PORT);
    }
    //不断循环连接从节点
    public void run() {
        String line;
        while(true){
            try {
                Socket new_region = region_socket.accept();
                Thread HandlerTHread = new Thread(new RegionSocketReceiveHandler(new_region));
                HandlerTHread.start();
            } catch (Exception e) {
                System.out.println("Region copy 接收异常：" + e.getMessage());
            }
        }
    }

    private class RegionSocketReceiveHandler implements Runnable {
        private Socket socket;
        public BufferedReader input = null;
        public PrintWriter output = null;

        public RegionSocketReceiveHandler(Socket socket) throws IOException{
            this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.output = new PrintWriter(socket.getOutputStream(), true);
            this.socket = socket;
        }   
        public void run() {
            String line;
            try {
                line = input.readLine();
                if(line.startsWith("[region] copy")) {
                    String[] info = line.split(" ");
                    String table_name = info[3];
                    try (
                        // Socket socket = region_socket.accept();
                        InputStream in = socket.getInputStream();
                        OutputStream out = new FileOutputStream(new File("../../../../" + table_name)) 
                    ){
                        byte[] buf = new byte[2048];
                        int len;    
                        while ((len = in.read(buf)) != -1) {
                            out.write(buf, 0, len);
                        }
                        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {
                            writer.write("[Region] copy " + table_name + " successfully");
                        }
                        
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

