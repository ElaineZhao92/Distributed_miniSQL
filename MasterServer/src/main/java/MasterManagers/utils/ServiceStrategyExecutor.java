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

    public boolean existServer(String hostUrl) {
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


    //主节点给负载小的从节点发送挂掉从节点的 ip 与所有的表
    private void execInvalidStrategy (String hostUrl) {
        System.out.println("---Invalid：hostUrl----");
        StringBuffer allTable = new StringBuffer();
        List<String> tableList = tableManager.getTableList(hostUrl);//获取tableManager中hostUrl的表格列表
        //[master] drop ip name name
        String bestInet = tableManager.getIdealServer(hostUrl);
        log.warn("bestInet:"+bestInet);
        allTable.append(hostUrl+" ");
        int i = 0;
        for(String s:tableList){
            if(i==0)
                allTable.append(s);
            else {
                allTable.append(" ");
                allTable.append(s);
            }
        }
        tableManager.exchangeTable(bestInet,hostUrl);
        SocketThread socketThread = tableManager.getSocketThread(bestInet);
        socketThread.sendToRegion("drop "+allTable);
    }

    //恢复策略,主节点给从节点发消息，让该从节点删除所有旧的表,从节点重新上线，
    private void execRecoverStrategy(String hostUrl) {
        System.out.println("---Recover:hostUrl----");
        tableManager.recoverServer(hostUrl);
        SocketThread socketThread = tableManager.getSocketThread(hostUrl);
        socketThread.sendToRegion("recover ");
    }


}


