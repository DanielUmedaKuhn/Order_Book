package network;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StressTest {
    public static void main(String[] args) {
        int numeroDeClientes = 50;
        ExecutorService pool = Executors.newFixedThreadPool(numeroDeClientes);

        System.out.println("Disparando " + numeroDeClientes + " clientes simultâneos...");

        for (int i = 0; i < numeroDeClientes; i++) {
            pool.submit(() -> {
                try (Socket socket = new Socket("127.0.0.1", 8080);
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                    
                    out.println("limit buy 30 100");
                    
                } catch (Exception e) {
                    System.err.println("Erro na conexão: " + e.getMessage());
                }
            });
        }
        pool.shutdown();
    }
}
