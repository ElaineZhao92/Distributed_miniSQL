import ClientManager.ClientManager;
import java.io.IOException;
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        ClientManager clientManager = new ClientManager();
        clientManager.run();
        // String line="[master] query result ip";
        // if (line.startsWith("[master]")) {
        //     // 截取ip地址
        //     String[] argss = line.split(" ");
        //     String ip = argss[3];
        //     System.out.println(ip+"");
        // }
    }
}
