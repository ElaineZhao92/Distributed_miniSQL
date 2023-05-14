package MasterManager.SocketManager;

import io.netty.util.internal.SocketUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import MasterManager.TableManager;

public class SocketManager {
    private TableManager tableManager;
    private ServerSocket serverSocket;
    public SocketManager(int PORT, TableManager tableManager) throws IOException, InterruptedException {
        this.tableManager = tableManager;
        this.serverSocket = new ServerSocket(PORT);
    }
    /**
     * 1. 主节点启动
     * 2. 从节点启动，先完成zookeeper的注册，再将本节点存储的表名通过socket都发给主节点，格式是<region>[1]name name name
     * 3. 等待从节点的表格更改消息<region>[2]name delete/add
     * 4. 等待客户端的表格查询信息<client>[1]name,返回<master>[1]ip
     * 5. 等待客户端的表格创建信息<client>[2]name,做负载均衡处理后返回<master>[2]ip
     * 6. 容错容灾，RegionServer挂了后给另一个合适的从节点发消息，格式是<master>[3]name@sql#name@sql#name@sql.从节点从ftp上下载完后
     *    给主节点发送"<region>[3]Complete disaster recovery"
     * 7. 从节点恢复重新上线，主节点给上线的从节点发送消息，格式是<master>[4]recover。从节点删除完自己本地所储存的表后，给主
     *    节点发送<region>[4]。
     */
    public void startWorking() throws InterruptedException, IOException {
        while(true){
            Thread.sleep(200);
            Socket socket = serverSocket.accept();
            SocketThread socketThread = new SocketThread(socket, tableManager);
            String ip = socket.getInetAddress().getHostAddress();
            if(ip.equals("127.0.0.1"))
                ip = SocketUtils.loopbackAddress().getHostAddress();
                // ip = SocketUtils.getHostAddress();
            Thread thread = new Thread(socketThread);
            thread.start();
        }
    }
}
