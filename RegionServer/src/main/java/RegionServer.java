import miniSQL.API;
import miniSQL.Interpreter;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

import javafx.scene.layout.Region;

public class RegionServer implements Runnable {
    private ClientSocketManager clientSocketManager;
    private DatabaseManager dataBaseManager;
    private MasterSocketManager masterSocketManager;
    private RegionSocketReceiveManager regionSocketReceiveManager;
    private ZookeeperServiceManager zookeeperManager;

    private final int PORT = 22222;

    public RegionServer() throws IOException, InterruptedException {
//        System.out.println("start!!!");
        dataBaseManager = new DatabaseManager();
        zookeeperManager = new ZookeeperServiceManager();
        masterSocketManager = new MasterSocketManager();
        masterSocketManager.sendTableInfoToMaster(dataBaseManager.getMetaInfo());
        clientSocketManager = new ClientSocketManager(PORT,masterSocketManager);
        regionSocketReceiveManager = new RegionSocketReceiveManager(1117);
        Thread centerThread = new Thread(clientSocketManager);
//        Socket socket = new Socket("10.192.134.67", 12345);
//        PrintWriter output = new PrintWriter(socket.getOutputStream(), true);
//
//        Thread RegionSocketSendThread = new Thread(new RegionSocketSendManager("10.192.134.67", "a_sql.txt", output));
//        RegionSocketSendThread.start();
        centerThread.start();

//        System.out.println("finish!!!");
    }

    public static void main(String[] args) throws Exception {
        RegionServer regionServer=new RegionServer();
//        System.out.println("regionserver finish!");
        regionServer.run();
    }

    public void run() {
//        System.out.println("run begin!!");
        try {
            API.initial();
//            System.out.println("api finish!!");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Thread zkServiceThread = new Thread(zookeeperManager);
        zkServiceThread.start();
//        System.out.println("zk finish!");
        Thread regionSocketReceiveThread = new Thread(regionSocketReceiveManager);
        regionSocketReceiveThread.start();
//        System.out.println("receive finish!");
        Thread MasterSocketThread = new Thread(masterSocketManager);
        MasterSocketThread.start();
//        System.out.println("masterso finish!");

        System.out.println("REGION> 从节点开始运行!");
    }
}
