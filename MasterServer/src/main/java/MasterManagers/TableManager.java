package MasterManagers;

import MasterManagers.SocketManager.SocketThread;
import com.google.common.collect.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * 记录所有连结果的ip
 * 记录当前活跃的ip，以及ip对应的table list
 * */
public class TableManager {
    private Map<String, String> TableInfo;
    private List<String> serverList;
    // Serverip -> table list
    private Map<String, List<String>> liveServer;
    // ip -> socket
    private Map<String, SocketThread> socketThreadMap;

    public TableManager() throws IOException {
        TableInfo = new HashMap<>();
        serverList = new ArrayList<>();
        liveServer = new HashMap<>();
        socketThreadMap = new HashMap<>();
    }

    // ---------- 有关 server 的------------

    /**
     * 找到当前最适合的Server 标准就是server对应的table list最少
     * @return
     */
    public String getIdealiServer(){
        Integer min = Integer.MAX_VALUE;
        String result = "";
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getValue().size() < min){
                min = entry.getValue().size();
                result = entry.getKey();
            }
        }
        return result;
    }

    public boolean hasServer(String hostURL){
        for(String s: serverList)
            if(s.equals(hostURL))
                return true;
        return false;
    }

    public void addServer(String hostURL){
        if(!hasServer(hostURL)){
            serverList.add(hostURL);
        }
        // 不仅仅要加上这个主机，还要为其添加一个列表 用来记录所有的table
        List<String> empty_list = new ArrayList<>();
        liveServer.put(hostURL, empty_list);
    }

    public String getInetAddress(String table){
        for(Map.Entry<String, String> entry : TableInfo.entrySet()){
            if(entry.getKey().equals(table)){
                return entry.getValue();
            }
        }
        return null;
    }


}
