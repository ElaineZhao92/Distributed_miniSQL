package ClientManager;

import java.util.HashMap;
import java.util.Map;

/**
 * 缓存管理
 */
public class CacheManager {
    //客户端缓存表 <table,server>
    private Map<String, String> fcache;//主
    private Map<String, String> scache;//副

    public CacheManager() {
        this.fcache = new HashMap<>();
        this.scache = new HashMap<>();
    }

    /**
     * 查询某张表是否存在客户端中，如果存在就直接返回表名
     * @param table 要查询的表名
     * @return
     */
    public String getfCache(String table) {
        /* 
        有缓存返回对应的server
        无缓存返回null 
        */
        if (this.fcache.containsKey(table)) {
            return this.fcache.get(table);
        }
        return null;
    }
    public String getsCache(String table) {
        /* 
        有缓存返回对应的server
        无缓存返回null 
        */
        if (this.scache.containsKey(table)) {
            return this.scache.get(table);
        }
        return null;
    }
    /**
     * 在客户端缓存中存储已知的表和所在的服务器
     * @param table 数据表的名称n
     * @param server 服务器的IP地址和端口号
     */
    public void setfCache(String table, String server) {
        fcache.put(table, server);
        System.out.println("存入缓存：table name" + table + " 主region IP:" + server);
    }
    public void setsCache(String table, String server) {
        scache.put(table, server);
        System.out.println("存入缓存：table name" + table + " 副region IP:" + server);
    }

    public void delCache(String table){
        /* 
       删除缓存中的table 
        */
        if(this.fcache.containsKey(table)){
            this.fcache.remove(table);
        }
        if(this.scache.containsKey(table)){
            this.scache.remove(table);
        }
    }
}
