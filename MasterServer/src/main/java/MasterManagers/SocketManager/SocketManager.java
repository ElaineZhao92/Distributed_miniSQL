package MasterManagers.SocketManager;

import MasterManagers.TableManager;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketManager {
    private TableManager tableManager;
    private ServerSocket serverSocket;
    public SocketManager(int PORT, TableManager tableManager) throws IOException, InterruptedException {
        this.tableManager = tableManager;
        this.serverSocket = new ServerSocket(PORT);
    }

    public void startWorking() throws InterruptedException, IOException {

    }
}
