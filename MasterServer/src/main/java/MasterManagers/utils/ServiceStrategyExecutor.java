package MasterManagers.utils;

import MasterManagers.SocketManager.SocketThread;
import MasterManagers.TableManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


/**
 * 1. 从节点每次创表、插入、删除，在完成操作后将所修改的表和索引传输到ftp。
 * 2. 如果从节点挂了，主节点监测到后寻找表少的从节点，向该从节点发送备份指令（包括挂了的从节点所存储的所有表），从节点将表
 *    和索引从ftp上读取下来。读取完成后给主节点发消息，主节点收到后修改 table-server map
 * 3. 如果从节点重新连上，执行恢复策略，主节点向从节点发送恢复指令，从节点收到后将本节点表全部删除，删除完成后给主节点发消息，
 *    主节点收到后将其状态变更为有效的从节点，恢复正常使用
 */
@Slf4j
public class ServiceStrategyExecutor {

    private TableManager tableManager;

    public ServiceStrategyExecutor(TableManager tableManger) {
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
                case DISCOVER:
                    execDiscoverStrategy(hostUrl);
                    break;
                case INVALID:
                    execInvalidStrategy(hostUrl);
                    break;
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    private void execInvalidStrategy (String hostUrl) {
        StringBuffer allTable = new StringBuffer();
        List<String> tableList = tableManager.getTableList(hostUrl);
        //<master>[3]ip#name@name@
        String bestInet = tableManager.getIdealServer(hostUrl);
        log.warn("bestInet:"+bestInet);
        allTable.append(hostUrl+"#");
        int i = 0;
        for(String s:tableList){
            if(i==0){
                allTable.append(s);
            }
            else {
                allTable.append("@");
                allTable.append(s);
            }
        }
        tableManager.exchangeTable(bestInet,hostUrl);
        SocketThread socketThread = tableManager.getSocketThread(bestInet);
        socketThread.sendToRegion("[3]"+allTable);
    }

    private void execDiscoverStrategy(String hostUrl) {

    }

    private void execRecoverStrategy(String hostUrl) {
        tableManager.recoverServer(hostUrl);
        SocketThread socketThread = tableManager.getSocketThread(hostUrl);
        socketThread.sendToRegion("[4]recover");
    }


}

