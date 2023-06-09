package MasterManagers;

import MasterManagers.SocketManager.SocketThread;
import com.google.common.collect.Table;

import java.io.IOException;
import java.util.*;

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
        Integer min = Integer.MAX_VALUE;
        String ip = "";
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getValue().size() < min){
                min = entry.getValue().size();
                ip = entry.getKey();
            }
        }
        min = Integer.MAX_VALUE;
        result.add(ip);

        // 只有一个region的情况，返回的主/副region的ip一样
        if(serverNum() == 1){
            result.add(ip);
            return result;
        }

        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getValue().size() < min && entry.getKey() != result.get(0)){
                min = entry.getValue().size();
                ip = entry.getKey();
            }
        }
        result.add(ip);

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
//            System.out.println(entry.getKey());
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
        System.out.println("MASTER>add Server OK!");
    }

    public void deleteServer(String hostURL){
        liveServer.remove(hostURL);
    }

    public int serverNum(){
        return liveServer.size();
    }

    public String getUniqueServer(){
        String hostURL = "";
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet())
            hostURL = entry.getKey();
        return hostURL;
    }

    public List<String> getInetAddress(String table){
        System.out.println("MASTER>get " + table + "'s region ips");
        for(Map.Entry<String, List<String>> entry : TableInfo.entrySet()){
            System.out.println(entry.getKey());
            List<String> ips = entry.getValue();
            for(String ip : ips)
                System.out.println(ip);
            if(entry.getKey().equals(table)){
                return ips;
            }
        }

//        System.out.println("-------------------------");
        return null;
    }

    public void recoverServer(String hostUrl) {
        List<String> temp = new ArrayList<>();
        liveServer.put(hostUrl,temp);
    }

    // ---------- 有关 table 的------------

    public void addTable(String table, String ip){
        System.out.println("MASTER>addTable::");
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

    /***
     * 当系统内只有一个region时候，需要把所有表TableInfo中原来没挂的那张region的ip给抹去。
     * 现在 主 副 region机制失效，变成只有主
     */
    public void exchangeTable(String region){
        for(Map.Entry<String, List<String>> entry : TableInfo.entrySet()){
            if (entry.getValue().get(0).equals(region))
                entry.getValue().set(1, region);
            else if (entry.getValue().get(1).equals(region))
                entry.getValue().set(0, region);
        }
    }

    /***
     *
     * @param newRegion
     * @param oldRegion
     * @param change_Table
     */
    public void exchangeTable(String newRegion, String oldRegion, String change_Table) {
        List <String> tableList = getTableList(oldRegion);
        // oldRegion 的 TableList 下的表格，要全部更新tableInfo

        List<String> ips = TableInfo.get(change_Table);
        ips.remove(oldRegion);
        ips.add(newRegion);
        TableInfo.put(change_Table,ips);
        System.out.println("after change ips = " + TableInfo.get(change_Table).get(0) + " " +  TableInfo.get(change_Table).get(1));
        // 给新的Region 更改 liveServer.TableList
        List <String> bestInetTable = liveServer.get(newRegion);
        bestInetTable.add(change_Table);
        liveServer.put(newRegion,bestInetTable);
        List <String> oldRegion_tables = liveServer.get(oldRegion);
        oldRegion_tables.remove(change_Table);
        liveServer.put(oldRegion,oldRegion_tables);
    }

    public List<String> getTableList(String hostUrl) {
        for(Map.Entry<String, List<String>> entry : liveServer.entrySet()){
            if(entry.getKey().equals(hostUrl)){
                return  entry.getValue();
            }
        }
        return null;
    }

    public String showTables(){
        String tables = "";
        for(Map.Entry<String, List<String>> entry : TableInfo.entrySet()){
            tables += entry.getKey() + " ";
        }
        return tables;
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
