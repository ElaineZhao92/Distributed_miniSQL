import miniSQL.API;
import miniSQL.Interpreter;

import java.io.IOException;

import javafx.scene.layout.Region;

public class RegionServer implements Runnable {
    private ClientSocketManager clientSocketManager;
    private DatabaseManager dataBaseManager;
    private MasterSocketManager masterSocketManager;
    private ZookeeperServiceManager zookeeperManager;

    private final int PORT = 22222;

    public RegionServer() throws IOException{
        dataBaseManager = new DatabaseManager();
        zookeeperManager = new ZookeeperServiceManager();
        masterSocketManager = new MasterSocketManager();
        clientSocketManager = new ClientSocketManager(PORT,masterSocketManager);
        masterSocketManager.sendToMaster(null);
        new Thread(clientSocketManager).start();
    }

    public static void main(String[] args) throws IOException {
        RegionServer regionServer=new RegionServer();
        new Thread(regionServer).start();
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
        Thread MasterSocketThread = new Thread(masterSocketManager);
        MasterSocketThread.start();

    }
}