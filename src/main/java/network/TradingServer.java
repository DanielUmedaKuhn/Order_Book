package network;
import engine.MatchingEngine;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;
import java.net.*;
import java.util.concurrent.atomic.AtomicLong;

//classe servidor para receber conexões de clientes
public class TradingServer {  
    private final int port;
    private final MatchingEngine engine;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(10);
    private static final AtomicLong idGenerator = new AtomicLong(System.currentTimeMillis());

    public TradingServer(int port, MatchingEngine engine){
        this.port = port;
        this.engine = engine;
    }

    //inicia o servidor
    public void start(){
        try(ServerSocket serverSocket = new ServerSocket(port)){
            System.out.println("[SERVER] - Motor de Negociação rodando na porta "  + port);

            while(true){
                //servidor espera alguém conectar
                Socket clientSocket = serverSocket.accept();
                System.out.println("[SERVER] - Novo cliente conectado: " + clientSocket.getInetAddress());
                threadPool.execute(() -> handleClient(clientSocket));  //joga handleClient para a pool de threads
            }
        }
        catch(IOException e){
            System.out.println("[SERVER] - Erro no servidor: " + e.getMessage());
        }
    }

    //trata as conexões de clientes, fazendo o processamento das orders
    private void handleClient(Socket socket){
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)){

            String inputLine;
            while((inputLine = in.readLine()) != null) {
                try {
                    String[] parts = inputLine.trim().split("\\s+");
                    if(parts[0].equalsIgnoreCase("CANCEL")){
                        long orderId = Long.parseLong(parts[1]);
                        boolean canceled = engine.cancelOrder(orderId);
                        out.println("Order " + orderId + (canceled ? " cancelled successfully." : " not found or already executed."));

                    } else if (parts.length >= 2 && parts[0].equalsIgnoreCase("PRINT") && parts[1].equalsIgnoreCase("BOOK")){
                        engine.printOrderBook();
                        out.println("Order Book printed.");

                    } else if (parts[0].equalsIgnoreCase("MODIFY")){
                        //coleta o ID da ordem, o novo preço e a nova quantidade
                        long oldOrderId = Long.parseLong(parts[1]);
                        long newPrice = Math.round(Double.parseDouble(parts[2]) * 100.0);  //multiplica por 100 para armazenar como inteiro e eliminar erro de arredondamento (ex: 100.50 -> 10050)
                        int newQty = Integer.parseInt(parts[3]);

                        long newOrderId = idGenerator.getAndIncrement();   //gera um novo ID para a ordem modificada

                        boolean modified = engine.modifyOrder(oldOrderId, newOrderId, newPrice, newQty);
                        out.println("Order " + oldOrderId + (modified ? (" modified. New ID: " + newOrderId + " " + newQty + " @ " + newPrice / 100.0) : " not found or already executed."));

                    } else if(parts[0].equalsIgnoreCase("PEG")){
                        String reference = parts[1].toLowerCase();
                        enums.Side side = enums.Side.valueOf(parts[2].toUpperCase());
                        int qty = Integer.parseInt(parts[3]);

                        if((reference.equals("bid") && side == enums.Side.SELL) || (reference.equals("offer") && side == enums.Side.BUY)){
                            out.println("ERRO: Order rejected. 'Peg bid' must be BUY and 'Peg offer' must be SELL to not cross the spread.");
                        } else {
                            long orderId = idGenerator.getAndIncrement();  //gera um novo ID para a ordem pegged
                            engine.enqueue(orderId, 0, qty, qty, side, enums.OrderType.PEGGED);
                            out.println("Order created: pegged to " + reference + " " + side + " " + qty + " " + orderId);
                        }
                    } else {
                        //coleta os parâmetros da ordem (type, side, price, quantity)
                        enums.OrderType type = enums.OrderType.valueOf(parts[0].toUpperCase());
                        enums.Side side = enums.Side.valueOf(parts[1].toUpperCase());
                        long price;
                        int qty;

                        if (type == enums.OrderType.MARKET){  //para market e pegged orders, o preço não é necessário
                            price = 0;
                            qty = Integer.parseInt(parts[2]);
                        } else {
                            price = Math.round(Double.parseDouble(parts[2]) * 100.0);  //multiplica por 100 para armazenar como inteiro e eliminar erro de arredondamento (ex: 100.50 -> 10050)
                            qty = Integer.parseInt(parts[3]);
                        }

                        //ID único baseado no tempo para cada order de rede
                        long orderId = idGenerator.getAndIncrement();

                        engine.enqueue(orderId, price, qty, qty, side, type);
                        
                        if (type == enums.OrderType.LIMIT){
                            out.println("Order created: " + side + " " + qty + " @ " + price / 100.0 + " " + orderId);
                        } else if (type == enums.OrderType.MARKET){  //print é diferente para market orders por não possuirem preço definido
                            out.println("Order created: " + side + " " + qty + " @ MARKET " + orderId);
                        }
                    }
                }
                catch(Exception e){
                    out.println("ERRO: Formato Inválido. Use: limit SIDE PRICE QTY (ex: limit buy 100 10), market SIDE QTY (ex: market buy 10), peg REFERENCE SIDE QTY (ex: peg bid buy 10 ou peg offer sell 10), modify ID NEW_PRICE NEW_QUANTITY (ex: modify 16954203 105 15) cancel ID (ex: cancel 16954203), book para imprimir o livro de ordens.");
                }
            }
        }
        catch(IOException e){
            System.out.println("[SERVER] Conexão encerrada com o cliente.");
        }
    }
}
