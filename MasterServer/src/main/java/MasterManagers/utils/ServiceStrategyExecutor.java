package MasterManagers.utils;

import MasterManagers.SocketManager.SocketThread;
import MasterManagers.TableManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


@Slf4j
public class ServiceStrategyExecutor {

    private TableManager tableManager;

    public ServiceStrategyExecutor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public boolean hasServer(String hostUrl) {
        return tableManager.hasServer(hostUrl);
    }

    public void execStrategy(String hostUrl, StrategyTypeEnum type) {
        try {
            switch (type) {
                case RECOVER:
                    execRecoverStrategy(hostUrl);
                    break;
                case INVALID:
                    execInvalidStrategy(hostUrl);
                    break;
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    /***
     *
     * @param hostUrl
     * master 先找到一个接替的Region，作为bestInet
     * 对于每一张表，master要联系他的副region，告诉副region新的bestInet和表名字，使得表能够再次备份
     * 发送给副Region的语句格式：[master] copy ip tableName
     * 这里首先要遍历挂掉Region的TableList
     */

    private void execInvalidStrategy (String hostUrl) {
        System.out.println("---Invalid：hostUrl----");
        StringBuffer allTable = new StringBuffer();
        List<String> tableList = tableManager.getTableList(hostUrl);//获取tableManager中hostUrl的表格列表
        // 获得除了当前ip之外，最佳的ip作为接任Region
        String bestInet = tableManager.getIdealServer(hostUrl);
        System.out.println("bestInet: " + bestInet);

        for(String table : tableList){
            String region = tableManager.getRegion1(hostUrl, table);
            String message = "[master] copy" + bestInet + table;
            SocketThread socketThread = tableManager.getSocketThread(region);
            socketThread.send(message);
        }
        // 这里的语句格式：hostURL
        tableManager.exchangeTable(bestInet, hostUrl);
    }

    //恢复策略,主节点给从节点发消息，让该从节点删除所有旧的表,从节点重新上线，
    private void execRecoverStrategy(String hostUrl) {
        System.out.println("---Recover:hostUrl----");
        tableManager.recoverServer(hostUrl);
        SocketThread socketThread = tableManager.getSocketThread(hostUrl);
        socketThread.send("recover ");
    }


}


