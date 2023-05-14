package MasterManager.utils;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import MasterManager.ZookeeperManager;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.CreateMode;

public class CuratorHolder {
    private CuratorFramework client = null;
    private ExecutorService pool = Executors.newFixedThreadPool(2);
    private String hostUrl = null;
    public CuratorHolder(){
        this.setUpConnection(ZookeeperManager.ZK_HOST);
    }
    public CuratorHolder(String hostUrl){
        this.setUpConnection(hostUrl);
    }

    public void setUpConnection(String hostUrl){
        this.hostUrl = hostUrl;
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        if (client == null){
            synchronized (this) {
                client = CuratorFrameworkFactory.builder().connectString(hostUrl)
                        .connectionTimeoutMs(ZookeeperManager.ZK_CONNECTION_TIMEOUT).sessionTimeoutMs(ZookeeperManager.ZK_SESSION_TIMEOUT)
                        .retryPolicy(retryPolicy).build();
                client.start();
            }
        }
    }

    /* 一些检查函数 */
    public void checkClientConnected(){
        if(client == null) this.setUpConnection(ZookeeperManager.ZK_HOST);
    }
    public boolean checkNodeExist(String path) throws Exception {
        checkClientConnected();
        Stat s = client.checkExists().forPath(path);
        if(s == null) return false;
        return true;
    }

    /**
     *
     * @param registerPath 节点路径
     * @param value        值
     * @return
     * @throws Exception
     */
    public String createNode(String registerPath, String value) throws Exception {
        checkClientConnected();
        // 如果父节点不存在 则先创建父节点
        return client.create().creatingParentsIfNeeded().forPath(registerPath, value.getBytes());
    }


    /**
     *
     * @param registerPath  节点路径
     * @param value         值
     * @param nodeType      节点类型
     * @return
     * @throws Exception
     */
    public String createNode(String registerPath, String value, CreateMode nodeType ) throws Exception {
        checkClientConnected();
        if(nodeType == null)
            throw new RuntimeException("节点类型不合法");
        else if(CreateMode.PERSISTENT.equals(nodeType))
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(registerPath, value.getBytes());
        else if(CreateMode.EPHEMERAL.equals((nodeType)))
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(registerPath, value.getBytes());
        else if(CreateMode.EPHEMERAL_SEQUENTIAL.equals(nodeType))
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(registerPath, value.getBytes());
        else if(CreateMode.PERSISTENT_SEQUENTIAL.equals(nodeType))
            return client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(registerPath, value.getBytes());
        else
            throw new RuntimeException("节点类型不采纳");
    }

    /**
     * 监听子节点
     *
     * @param targetPath
     * @return
     */
    public void monitorChildrenNodes(String targetPath,PathChildrenCacheListener listener) throws Exception {
        CuratorCache cache = CuratorCache.build(client, targetPath);
        cache.start();
        CuratorCacheListener listener2 = CuratorCacheListener.builder().forPathChildrenCache(targetPath, client, listener).build();
        cache.listenable().addListener(listener2);
    }

    /**
     * 获取单个节点
     *
     * @param targetPath
     * @return
     */
    public String getData(String targetPath) throws Exception {
        checkClientConnected();
        return new String(client.getData().forPath(targetPath));
    }

    /**
     * 获取子节点列表
     *
     * @param targetPath
     * @return 子节点名字列表
     */
    public List<String> getChildren(String targetPath) throws Exception {
        checkClientConnected();
        return client.getChildren().forPath(targetPath);
    }
}


