
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import MasterManagers.ZookeeperManager;
import MasterManagers.utils.CuratorHolder;
import MasterManagers.utils.SocketUtils;


import java.util.Enumeration;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;

@Slf4j
public class ZookeeperServiceManager implements Runnable {
    @Override
    public void run() {
        this.serviceRegister();
    }

    private void serviceRegister() {
        try { // 向ZooKeeper注册临时节点
            CuratorHolder curatorClientHolder = new CuratorHolder();
            int nChildren = curatorClientHolder.getChildren(ZookeeperManager.ZNODE).size();
            if(nChildren==0)
                curatorClientHolder.createNode(getRegisterPath() + nChildren, SocketUtils.getHostAddress(), CreateMode.EPHEMERAL);
            else{
                String index = String.valueOf(Integer.parseInt((curatorClientHolder.getChildren(ZookeeperManager.ZNODE)).get(nChildren - 1).substring(7)) + 1);
                curatorClientHolder.createNode(getRegisterPath() + index, SocketUtils.getHostAddress(), CreateMode.EPHEMERAL);
            }

            synchronized (this) { // 阻塞该线程，直到主动退出或者发生异常
                wait();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }
    private static String getRegisterPath() {
        return ZookeeperManager.ZNODE + "/" + ZookeeperManager.HOST_NAME_PREFIX;
    }
}

