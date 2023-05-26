package MasterManagers.SocketManager;

import MasterManagers.TableManager;
import MasterManagers.utils.ServiceStrategyExecutor;
import MasterManagers.utils.SocketUtils;
import MasterManagers.utils.StrategyTypeEnum;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;

import javax.swing.plaf.synth.Region;
import java.net.Socket;

@Slf4j
public class RegionCMD {
    private TableManager tableManager;
    private Socket socket;

    public RegionCMD(TableManager tableManager, Socket socket){
        this.tableManager = tableManager;
        this.socket = socket;
    }

    public String processRegionCommand(String cmd){
        String result = "";
        String ip = socket.getInetAddress().getHostAddress();

        System.out.println(ip);

//        if (cmd.startsWith("recover") && !tableManager.hasServer(ip)) {
//            tableManager.addServer(ip);
//            String[] allTable = cmd.substring(6).split(" ");
//            for(String temp : allTable) {
//                tableManager.addTable(temp, ip);
//            }
//            System.out.println("------add server ok-----");
//            result += "recover"; //不管region是第几次连，让他drop本地所有的内容
//
//        }
//        else if (cmd.startsWith("recover") && tableManager.hasServer(ip) &&!tableManager.inLiveServer(ip)) {
//            System.out.println("-----not in Live Server------");
//            String[] allTable = cmd.substring(6).split(" ");
//            ServiceStrategyExecutor executor = new ServiceStrategyExecutor(tableManager);
//            executor.execStrategy(ip, StrategyTypeEnum.RECOVER);
//            System.out.println("------Exec Recover, add Tables okay!-----");
//            for(String temp : allTable) {
//                tableManager.addTable(temp, ip);
//            }
//            result += "recover"; //不管region是第几次连，让他drop本地所有的内容
//        }   这段 recover 在 Zookeeper 这里实现
        if (cmd.startsWith("create")) {

            String tableName = cmd.substring(7);
            tableManager.addTable(tableName,ip);
            result += "create table " + tableName + "\n";
            System.out.println("result = " + result);

        }
        else if(cmd.startsWith("delete")){
            String tableName = cmd.substring(7);
            tableManager.deleteTable(tableName,ip);
            result += "delete table " + tableName + "\n";
            System.out.println("result = " + result);
        }
        else if (cmd.startsWith("drop")){
            log.warn("完成从节点的数据转移");

        } else if (cmd.startsWith("recover")){
            log.warn("完成从节点的恢复，重新上线");

        }

        return result;
    }

}
