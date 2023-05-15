package MasterManagers.SocketManager;

import MasterManagers.TableManager;
import com.google.common.collect.Table;
import io.netty.util.internal.SocketUtils;
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
        if(ip.equals("127.0.0.1"))
            ip = SocketUtils.loopbackAddress().getHostAddress();
        if (cmd.startsWith("query") && !tableManager.hasServer(ip)) {
            tableManager.addServer(ip);
            String[] allTable = cmd.substring(6).split(" ");
            for(String temp : allTable) {
                tableManager.addTable(temp, ip);
            }
        } else if (cmd.startsWith("add")) {
            // [region] add/delete table_name 情况
            String tableName = cmd.substring(4);
            tableManager.addTable(tableName,ip);
            result += "add Table" + tableName + "\n";

        } else if (cmd.startsWith("delete")) {
            String tableName = cmd.substring(7);
            tableManager.deleteTable(tableName,ip);
            result += "delete Table" + tableName + "\n";

        } else if (cmd.startsWith("drop")){
            log.warn("完成从节点的数据转移");

        } else if (cmd.startsWith("recover")){
            log.warn("完成从节点的恢复，重新上线");

        }

        return result;
    }

}
