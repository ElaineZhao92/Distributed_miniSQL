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
    private Map<String, List<String>> TableInfo;  // table -> ip1 ip2
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
    public List<String> getIdealServer(){
        List<String> result = new ArrayList<>();
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()) {
            System.out.println(entry.getKey());
        }

        for (int i = 0 ; i < 2; i ++ ){
            Integer min = Integer.MAX_VALUE;
            for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
                if(entry.getValue().size() < min){
                    if(i == 1 && (entry.getKey() == result.get(0)))
                        continue;
                    min = entry.getValue().size();
                    result.add(entry.getKey());
                    System.out.println(entry.getKey());
                }
            }
        }
        System.out.println(result.get(0) + " " + result.get(1));
        return result;
    }

    /**
     *
     * @param hostURL
     * @return 合适的region ip作为接替
     * 这里只需要返回一个就行，作为迁移的Region
     */
    public String getIdealServer(String hostURL, String table){
        Integer min = Integer.MAX_VALUE;
        String result = "";
        String region1 = getRegion1(hostURL, table);
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            System.out.println(entry.getKey());
            if(!(entry.getKey().equals(hostURL)) && !(entry.getKey().equals(region1)) && entry.getValue().size()<min){
                min = entry.getValue().size();
                result = entry.getKey();
            }
        }
        return result;
    }

    public boolean hasServer(String hostURL){
        boolean result = false;
        System.out.println("ServerLists: -----");
        for(String s: serverList){
            System.out.println("    " + s);
            if(s.equals(hostURL))
                result = true;
        }
        return result;
    }

    public boolean inLiveServer(String hostURL) {
        for (String key : liveServer.keySet()) {
            if (hostURL.equals(liveServer.get(key)))
                return true;
        }
        return false;
    }

    public void addServer(String hostURL){
        if(!hasServer(hostURL)){
            serverList.add(hostURL);
        }
        // 不仅仅要加上这个主机，还要为其添加一个列表 用来记录所有的table
        List<String> empty_list = new ArrayList<>();
        liveServer.put(hostURL, empty_list);
        System.out.println("---add Server OK!---");
    }

    public List<String> getInetAddress(String table){
        System.out.println("-----get " + table + "'s region ips-----");
        for(Map.Entry<String, List<String>> entry : TableInfo.entrySet()){
            System.out.println(entry.getKey());
            List<String> ips = entry.getValue();
            for(String ip : ips)
                System.out.println(ip);
            if(entry.getKey().equals(table)){
                return ips;
            }
        }

        System.out.println("-------------------------");
        return null;
    }

    public void recoverServer(String hostUrl) {
        List<String> temp = new ArrayList<>();
        liveServer.put(hostUrl,temp);
    }

    // ---------- 有关 table 的------------

    public void addTable(String table, String ip){
        System.out.println("------addTable::-------");
        System.out.println(table + "," + ip);
        if(TableInfo.containsKey(table)){
            List<String> ips = TableInfo.get(table);
            ips.add(ip);
        }
        else{
            List<String> ips = new ArrayList<>();
            ips.add(ip);
            TableInfo.put(table, ips);
        }
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

    public void exchangeTable(String newRegion, String oldRegion) {
        List <String> tableList = getTableList(oldRegion);
        // oldRegion 的 TableList 下的表格，要全部更新tableInfo
        for(String table : tableList){
            List<String> ips = TableInfo.get(table);
            ips.remove(oldRegion);
            ips.add(newRegion);
            TableInfo.put(table,ips);
        }
        // 给新的Region 建立liveServer.TableList
        List <String> bestInetTable = liveServer.get(newRegion);
        bestInetTable.addAll(tableList);
        liveServer.put(newRegion,bestInetTable);
        liveServer.remove(oldRegion);
    }

    public List<String> getTableList(String hostUrl) {
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getKey().equals(hostUrl)){
                return  entry.getValue();
            }
        }
        return null;
    }

    public String getRegion1(String hostUrl, String tableName){
        List<String> regions = TableInfo.get(tableName);
        if (regions.get(0).equals(hostUrl))
            return regions.get(1);
        else
            return regions.get(0);
    }

    //--------------------Socket---------------------
    public void addSocketThread(String hostUrl, SocketThread socketThread) {
        socketThreadMap.put(hostUrl,socketThread);
    }
    public SocketThread getSocketThread(String hostUrl) {
        for(Map.Entry<String, SocketThread> entry : socketThreadMap.entrySet()){
            if(entry.getKey().equals(hostUrl))
                return entry.getValue();
        }
        return null;
    }

}
