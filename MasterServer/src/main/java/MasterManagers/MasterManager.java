package MasterManagers;
import MasterManagers.SocketManager.*;

import java.io.IOException;

public class MasterManager {
    private ZookeeperManager zookeeperManager;
    private SocketManager socketManager;
    private TableManager tableManager;

    private final int PORT = 66666;

    public MasterManager() throws IOException, InterruptedException {
        tableManager = new TableManager();
        socketManager = new SocketManager(PORT, tableManager);
        zookeeperManager = new ZookeeperManager(tableManager);
    }
    public void initialize() throws IOException, InterruptedException{
        // 建立线程，向Zookeeper发送请求，获得ZNODE目录下的信息并且持续监控，如果发生了目录的变化则执行回调函数，处理相应策略。
        Thread zookeeperServiceThread = new Thread(zookeeperManager);
        zookeeperServiceThread.start();

        // 这个线程负责处理从节点之间的通信，来响应客户端的请求。
        socketManager.startWorking();
    }
}
