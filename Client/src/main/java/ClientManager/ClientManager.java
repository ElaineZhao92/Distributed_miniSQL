package ClientManager;

import ClientManager.MasterSocketManager;
import ClientManager.RegionSocketManager;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * 客户端管理程序
 */

public class ClientManager {

    public CacheManager cacheManager;
    public MasterSocketManager masterSocketManager;
    public RegionSocketManager regionSocketManager;

    public ClientManager() throws IOException {
        // 绑定一个cacheManager
        this.cacheManager = new CacheManager();
        this.masterSocketManager = new MasterSocketManager(this);
        this.regionSocketManager = new RegionSocketManager();
    }

    // 在客户端做一个简单的sqlParser，先对sql语句进行一个简单的解析，然后在客户端缓存中查询表是否已经存在
    public void run()
            throws IOException, InterruptedException {
        System.out.println("CLIENT>>>Client start");
        Scanner input = new Scanner(System.in);
        String line = "";
        while (true) {
            StringBuilder sql = new StringBuilder();
            // 读入一句完整的SQL语句
            System.out.println("CLIENT>>>please input the sql:");
            // System.out.print("DisMiniSQLCIENT>>>");
            while (line.isEmpty() || line.charAt(line.length() - 1) != ';') {
                line = input.nextLine();
                if (line.isEmpty()) {
                    //System.out.print("          CLIENT>>>");
                    continue;
                }
                //System.out.print("          CLIENT>>>");
                sql.append(line);
                sql.append(' ');
            }
            line = "";
            System.out.println(sql.toString());
            if (sql.toString().trim().equals("quit;")) {
                this.masterSocketManager.closeMasterSocket();
                if (this.regionSocketManager.socket != null) {
                    this.regionSocketManager.closeRegionSocket();
                }
                break;
            }

            // 获得目标表名和索引名
            String command = sql.toString();
            System.out.println("command");
            sql = new StringBuilder();
            Map<String, String> target = this.sqlParser(command);
            if (target.containsKey("error")) {
                System.out.println("CLIENT>>>the format is wrong, please try again!");
            }

            String table = target.get("name");
            String fcache = null;
            String scache = null;
            System.out.println("CLIENT>>>the table is: " + table);

            if (target.get("kind").equals("create")) {
                //创表无cache 
                this.masterSocketManager.processCreate(command,target.get("kind"), table);
            } else {
                //有缓存的情况 进入表中查询
                if (target.get("cache").equals("true")) {
                    fcache = cacheManager.getfCache(table);
                    if (fcache == null) {
                        System.out.println("CLIENT>>>There is no corresponding cache for the table .");
                         // cache里面没有找到表所对应的端口号，去masterSocket里面查询
                        // this.masterSocketManager.process(command, table);
                    } else {
                        //终于查到了！直接在RegionSocketManager中进行连接
                        System.out.println("CLIENT>>>The server according to the cache is: " + fcache);   
                        // this.connectToRegion(cache, command);
                    }
                }
                if (fcache == null) {
                    this.masterSocketManager.process(command,target.get("kind"), table);
                } else {
                    // 如果查到了端口号就直接在RegionSocketManager中进行连接
                    this.connectToRegion(fcache, command);
                    scache = cacheManager.getsCache(table);
                    this.connectToRegion(scache, command);
                }


            }
        }
    }

    // use port connect to the region
    // drop
    // public void connectToRegion(int PORT, String sql) throws IOException, InterruptedException {
    //     this.regionSocketManager.connectRegionServer(PORT);
    //     Thread.sleep(100);
    //     this.regionSocketManager.sendToRegion(sql);
    // }

    // use ip connect to the region ,端口号固定为22222
    public void connectToRegion(String ip, String sql) throws IOException, InterruptedException, SocketException {
        try{
            if (this.regionSocketManager.connectRegionServer(ip)){
                Thread.sleep(100);
                this.regionSocketManager.sendToRegion(sql);
            }
            else{
                Map<String, String> target = this.sqlParser(sql);
                this.masterSocketManager.process(sql,target.get("kind"), target.get("name"));
            }

        }
        catch(SocketException e){
            //region 挂了重新进master处理
            System.out.println("进入catch 处理");

        }
    }
    // parse the sql
    public Map<String, String> sqlParser(String sql) {
        // 粗略地解析需要操作的table和index的名字
        Map<String, String> result = new HashMap<>();
        result.put("cache", "true");
        // 空格替换
        sql = sql.replaceAll("\\s+", " " );
        String[] words = sql.split(" ");
        // SQL语句的种类
        result.put("kind", words[0]);
        if (words[0].equals("create")) {
            // 对应create table xxx和create index xxx
            // 此时创建新表，不需要cache
            result.put("cache", "false");
            result.put("name", words[2]);
        } else if (words[0].equals("drop") || words[0].equals("insert") || words[0].equals("delete")) {
            // 这三种都是将table和index放在第三个位置的，可以直接取出
            String name = words[2].replace("(", "")
                    .replace(")", "").replace(";", "");
            result.put("name", name);
        } else if (words[0].equals("select")) {
            // select语句的表名放在from后面
            for (int i = 0; i < words.length; i ++) {
                if (words[i].equals("from") && i != words.length - 1) {
                    result.put("name", words[i + 1]);
                    break;
                }
            }
        }
        // 如果没有发现表名就说明出出现错误
        if (!result.containsKey("name")) {
            result.put("error", "true");
        }
        return result;
    }

}
