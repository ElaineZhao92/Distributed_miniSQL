package MasterManagers;

import MasterManagers.SocketManager.SocketThread;

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
    private Map<String, List<String>> liveServer; // Serverip -> table list
    private Map<String, SocketThread> socketThreadMap;  // ip -> socket

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
    public String getIdealServer(){
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

    public String getIdealServer(String hostURL){
        Integer min = Integer.MAX_VALUE;
        String result = "";
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(!entry.getKey().equals(hostURL) && entry.getValue().size()<min){
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

    public SocketThread getSocketThread(String hostUrl) {
        for(Map.Entry<String, SocketThread> entry : socketThreadMap.entrySet()){
            if(entry.getKey().equals(hostUrl))
                return entry.getValue();
        }
        return null;
    }

    public void recoverServer(String hostUrl) {
        List<String> temp = new ArrayList<>();
        liveServer.put(hostUrl,temp);
    }

    // ---------- 有关 table 的------------

    public void addTable(String table, String ip){
        TableInfo.put(table,ip);
        // 如果当前的ip已存在活跃列表中，那么将table计入其对应的list里保存。
        if(liveServer.containsKey(ip))
            liveServer.get(ip).add(table);
        else{ // 先新建一个空的列表，然后将这一新的ip<->list对存入
            List<String> empty_list = new ArrayList<>();
            empty_list.add(table);
            liveServer.put(ip, empty_list);
        }
    }

    public void deleteTable(String table, String ip){
        TableInfo.remove(table);
        liveServer.get(ip).removeIf(table::equals);
    }

    public void exchangeTable(String bestInet, String hostUrl) {
        List <String> tableList = getTableList(hostUrl);
        for(String table : tableList){
            TableInfo.put(table,bestInet);
        }
        List <String> bestInetTable = liveServer.get(bestInet);
        bestInetTable.addAll(tableList);
        liveServer.put(bestInet,bestInetTable);
        liveServer.remove(hostUrl);
    }

    public List<String> getTableList(String hostUrl) {
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getKey().equals(hostUrl)){
                return  entry.getValue();
            }
        }
        return null;
    }
}
