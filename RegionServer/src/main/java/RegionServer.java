import miniSQL.API;
import miniSQL.Interpreter;

import java.io.IOException;

import javafx.scene.layout.Region;

public class RegionServer implements Runnable {
    private ClientSocketManager clientSocketManager;
    private DatabaseManager dataBaseManager;
    private MasterSocketManager masterSocketManager;
    private RegionSocketReceiveManager regionSocketReceiveManager;
    private ZookeeperServiceManager zookeeperManager;

    private final int PORT = 22222;

    public RegionServer() throws IOException, InterruptedException {
        dataBaseManager = new DatabaseManager();
        zookeeperManager = new ZookeeperServiceManager();
        masterSocketManager = new MasterSocketManager();
        masterSocketManager.sendTableInfoToMaster(dataBaseManager.getMetaInfo());
        clientSocketManager = new ClientSocketManager(PORT,masterSocketManager);
        regionSocketReceiveManager = new RegionSocketReceiveManager(1117);
        Thread centerThread = new Thread(clientSocketManager);
        centerThread.start();
    }

    public static void main(String[] args) throws Exception {
        RegionServer regionServer=new RegionServer();
        regionServer.run();
    }

    public void run() {
        try {
            API.initial();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Thread zkServiceThread = new Thread(zookeeperManager);
        zkServiceThread.start();
        Thread regionSocketReceiveThread = new Thread(regionSocketReceiveManager);
        regionSocketReceiveThread.start();
        Thread MasterSocketThread = new Thread(masterSocketManager);
        MasterSocketThread.start();

        System.out.println("从节点开始运行!");
    }
}
