package MasterManagers.utils;


import MasterManagers.TableManager;
import MasterManagers.ZookeeperManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;



/**
 * ZooKeeper的节点监视器，将发生的事件进行处理，
 */
@Slf4j
public class ServiceMonitor implements PathChildrenCacheListener {

    private CuratorHolder client;
    private ServiceStrategyExecutor strategyExecutor;
    private TableManager tableManager;

    public ServiceMonitor(CuratorHolder curatorClientHolder, TableManager tableManager) {
        this.tableManager = tableManager;
        this.strategyExecutor = new ServiceStrategyExecutor(tableManager);
        this.client = curatorClientHolder;
    }

    //当子节点的事件发生时，childEvent()方法会被调用
    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
        String eventPath = pathChildrenCacheEvent.getData() != null ? pathChildrenCacheEvent.getData().getPath() : null;
        System.out.println("------ZooKeeper监听中-------");
        switch (pathChildrenCacheEvent.getType()) {
            case CHILD_ADDED:
                System.out.println("------Child ADDED-------");
                System.out.println("MASTER>服务器目录新增节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerAppear(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        client.getData(eventPath));
                break;
            case CHILD_REMOVED:
                System.out.println("------Child REMOVED-------");
                System.out.println("MASTER>服务器目录删除节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerDisappear(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        new String(pathChildrenCacheEvent.getData().getData()));
                break;
            case CHILD_UPDATED:
                System.out.println("------Child UPDATED-------");
                System.out.println("MASTER>服务器目录更新节点: " + pathChildrenCacheEvent.getData().getPath());
                eventServerUpdate(
                        eventPath.replaceFirst(ZookeeperManager.ZNODE + "/", ""),
                        client.getData(eventPath));
                break;
            default:
        }
    }

    /**
     * 处理服务器节点出现事件
     *
     * @param hostName
     * @param hostUrl
     */
    public void eventServerAppear(String hostName, String hostUrl) {
        System.out.println("MASTER>新增服务器节点：主机名" + hostName + ", 地址 " + hostUrl);
        if (strategyExecutor.hasServer(hostUrl)) {
            // 该服务器已经存在，即从失效状态中恢复
            System.out.println("对该服务器" + hostName + "执行恢复策略");
            strategyExecutor.execStrategy(hostUrl, StrategyTypeEnum.RECOVER);
        } else {
            // 新发现的服务器，新增一份数据
            System.out.println("MASTER>对该服务器" + hostName + "执行新增策略");
            strategyExecutor.execStrategy(hostUrl, StrategyTypeEnum.ADD);
        }
    }

    /**
     * 处理服务器节点失效事件
     *  @param hostName
     * @param hostUrl*/
    public void eventServerDisappear(String hostName, String hostUrl) {
        System.out.println("MASTER>服务器节点失效：主机名 " + hostName + ", 地址 " + hostUrl);
        if (!strategyExecutor.hasServer(hostUrl)) {
            throw new RuntimeException("需要删除信息的服务器不存在于服务器列表中");
        } else {
            // 更新并处理下线的服务器
            System.out.println("MASTER>对该服务器" + hostName + "执行负载失败策略");
            System.out.println("MASTER>将该服务器下所处理的表，负载均衡至其他活跃服务器");
            strategyExecutor.execStrategy(hostUrl, StrategyTypeEnum.INVALID);
        }
    }

    /**
     * 处理服务器节点更新事件
     *
     * @param hostName
     * @param hostUrl
     */
    public void eventServerUpdate(String hostName, String hostUrl) {
        System.out.println("MASTER>更新服务器节点：主机名 " + hostName + ",地址 " + hostUrl);
    }
}
