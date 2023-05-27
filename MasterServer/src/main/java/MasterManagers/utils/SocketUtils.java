package MasterManagers.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class SocketUtils {
    /**
     * 获得本机IP
     *
     */
    public static String getHostAddress() {
        try{
            InetAddress address = InetAddress.getLocalHost();
            return address.getHostAddress();
        }catch(UnknownHostException e){
            e.printStackTrace();
        }
        return null;
    }
}
