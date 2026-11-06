package engine;
import enums.OrderType;
import model.Order;
import model.Trade;
import enums.Side;
import database.OrderDAO;
import database.PersistenceWorker;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class MatchingEngine {
    private final OrderBook book = new OrderBook();
    private final OrderDAO orderDAO = new OrderDAO();
    private final PersistenceWorker dbWorker = new PersistenceWorker();
    
    //ring buffer guarda somente o comando da rede (DTO)
    private final OrderCommand[] ringBuffer = new OrderCommand[1024];
    private final AtomicLong producer = new AtomicLong();
    private final AtomicLong consumer = new AtomicLong();

    //object pool para as ordens, suporta até 100 mil ordens
    private final Order[] orderPool = new Order[100000];
    private int poolIndex = 99999;

    private final Set<Long> peggedBids = new HashSet<>(); 
    private final Set<Long> peggedAsks = new HashSet<>(); 

    public MatchingEngine(){
        //pré-alocação dos comandos no ring buffer
        for (int i = 0; i < ringBuffer.length; i++) {
            ringBuffer[i] = new OrderCommand(); 
        }
        
        //pré-alocação das ordens na pool
        for (int i = 0; i < orderPool.length; i++) {
            orderPool[i] = new Order();
        }

        Thread t = new Thread(dbWorker);
        t.setName("DB-Persistence_Thread");
        t.setDaemon(true);
        t.start();

        Runnable coreRunnable = this::run;
        Thread t2 = new Thread(coreRunnable);
        t2.setName("Core_Thread");
        t2.setDaemon(true);
        t2.start();
    }

    //método O(1) para pegar uma ordem da pool
    private synchronized Order borrowOrder() {
        if (poolIndex >= 0) {
            return orderPool[poolIndex--];
        }
        return new Order(); //fallback de segurança se extrapolar 100 mil makers
    }

    //método também O(1) para devolver uma ordem para a pool
    private synchronized void returnOrder(Order order) {
        if (poolIndex < orderPool.length - 1) {
            orderPool[++poolIndex] = order;
        }
    }

    public void rebuildBookFromDatabase(){
        System.out.println("[CORE] - Carregando orders abertas do banco de dados.");
        List<Order> openOrders = orderDAO.findAllOpen();

        for(Order order : openOrders){
            book.addOrder(order);

            if(order.type == OrderType.PEGGED){
                if(order.side == Side.BUY){
                    peggedBids.add(order.id);
                } else {
                    peggedAsks.add(order.id);
                }
            }
        }

        System.out.println("[CORE] - Recuperação concluída. " + openOrders.size() + " orders em memória.");
    }

    public void enqueue(long id, long price, int initialQuantity, int currentQuantity, Side side, OrderType type){
        long currentProducer = producer.getAndIncrement();
       
        while(true){
            long currentConsumer = consumer.get();

            if(currentProducer - currentConsumer < ringBuffer.length){
                int index = (int)(currentProducer & 1023);
                
                //preenche apenas o comando (mensageiro)
                OrderCommand cmd = ringBuffer[index];
                cmd.id = id;
                cmd.price = price;
                cmd.initialQuantity = initialQuantity;
                cmd.quantity = currentQuantity;
                cmd.side = side;
                cmd.type = type;
                break;
            } else {
                Thread.yield();  //espera a fila ter espaço
            }
        }
    }

    public void run(){
        while (true){
            long currentProducer = producer.get();
            long currentConsumer = consumer.get();

            if(currentConsumer < currentProducer){
                int index = (int)(currentConsumer & 1023);
                OrderCommand processingCmd = ringBuffer[index];   //leitura do comando
                if(processingCmd != null){
                    consumer.incrementAndGet();
                    submitOrder(processingCmd);   //submissão do comando
                } else { 
                    Thread.yield();
                }
            } else {
                Thread.yield();  //espera a fila ter elementos
            }
        }
    }

    private void updatePeggedOrders(Set<Long> peggedIds, long newPrice, Side side){
        Iterator<Long> it = peggedIds.iterator();
        while(it.hasNext()){ 
            long id = it.next();
            Order pOrder = book.getOrder(id);
            if(pOrder == null || pOrder.getQuantity() <= 0){
                it.remove();
                continue;
            }

            if (pOrder.price != newPrice){ 
                var sideMap = (side == Side.BUY) ? book.bids : book.asks; 
                PriceLevelQueue queue = sideMap.get(pOrder.price); 
                if (queue != null){
                    queue.remove(pOrder);  
                    if(queue.isEmpty()){
                        sideMap.remove(pOrder.price); 
                    }
                }

                pOrder.price = newPrice; 
                book.addOrder(pOrder); 
                
                // Passa pOrder.type em vez de false
                dbWorker.enqueueOrder((byte) 1, pOrder.id, pOrder.price, pOrder.getInitialQuantity(), pOrder.getQuantity(), side == Side.BUY, pOrder.type);
            }   
        }
    }
    
    //recebe o comando DTO e gera a ordem (entidade)
    public List<Trade> submitOrder(OrderCommand cmd) {
        if(cmd.price < 0 || cmd.initialQuantity <= 0 || cmd.quantity <= 0){
            System.out.println("Erro: Não são aceitos valores negativos ou iguais a zero.");
            return java.util.Collections.emptyList();  //retorna uma lista vazia para indicar que a ordem não foi processada, evita NullPointerException
        }

        long startTime = System.nanoTime();
    
        metrics.MetricsRegistry.totalOrders.increment();
        
        //transfere os dados do comando para uma ordem na object pool
        Order incoming = borrowOrder();
        incoming.reset(cmd.id, cmd.price, cmd.initialQuantity, cmd.quantity, cmd.side, cmd.type);
        
        List<Trade> trades = new ArrayList<>(); 

        long oldBestBid = getBestReferencePrice(book.bids);
        long oldBestAsk = getBestReferencePrice(book.asks);

        if (incoming.type == OrderType.PEGGED){
            // >>> NOVO: preço de referência do lado da ordem (0 = não há ordem LIMIT nesse lado)
            long reference = (incoming.side == Side.BUY) ? oldBestBid : oldBestAsk;

            // >>> ALTERADO: só rejeita a pegged NOVA (preço 0, vinda do comando PEG) sem referência.
            // Antes rejeitava também a que vinha de uma modificação, e a ordem sumia do livro.
            if(reference == 0 && incoming.price == 0){
                System.out.println("Erro: Ordem PEGGED rejeitada. Não há liquidez de referência no livro.");
                returnOrder(incoming);  //devolve a ordem para a pool
                return trades;          //aborta a execução e retorna uma lista vazia
            }

            // >>> ALTERADO: com referência, reatrela ao topo; sem referência (veio de modificação),
            // mantém o preço atual até o topo voltar a existir
            if(reference != 0){
                incoming.price = reference;
            }

            
            (incoming.side == Side.BUY ? peggedBids : peggedAsks).add(incoming.id);
        }

        dbWorker.enqueueOrder((byte) 0, incoming.id, incoming.price, incoming.getInitialQuantity(), incoming.getQuantity(), incoming.side == Side.BUY, incoming.type);  

        if (incoming.side == Side.BUY) {
            match(incoming, book.asks, trades);
        } else {
            match(incoming, book.bids, trades);
        }

        if (incoming.getQuantity() > 0 && (incoming.type == OrderType.LIMIT || incoming.type == OrderType.PEGGED)) {
            dbWorker.enqueueOrder((byte) 1, incoming.id, incoming.price, incoming.getInitialQuantity(), incoming.getQuantity(), incoming.side == Side.BUY, incoming.type);
            book.addOrder(incoming);
        } else {
            //se a ordem for descartada mas ainda possuir saldo, é marcada como CANCELLED no banco de dados
            if(incoming.getQuantity() == 0){
                //se a quantidade da ordem zerou, a ordem foi executada integralmente
                dbWorker.enqueueOrder((byte) 1, incoming.id, incoming.price, incoming.getInitialQuantity(), incoming.getQuantity(), incoming.side == Side.BUY, incoming.type);            
            } else {
                //se há saldo, mas foi descartada (Market), marcada como CANCELLED
                dbWorker.enqueueOrder((byte) 3, incoming.id, incoming.price, incoming.getInitialQuantity(), incoming.getQuantity(), incoming.side == Side.BUY, incoming.type);
            }

            //se a ordem já foi executada integralmente ou for Market, volta pra piscina
            returnOrder(incoming);
        }

        long newBestBid = getBestReferencePrice(book.bids);
        long newBestAsk = getBestReferencePrice(book.asks);

        if(newBestBid != oldBestBid && newBestBid != 0){
            updatePeggedOrders(peggedBids, newBestBid, Side.BUY);
        }
        if(newBestAsk != oldBestAsk && newBestAsk != 0){
            updatePeggedOrders(peggedAsks, newBestAsk, Side.SELL);
        }

        long endTime = System.nanoTime();
        System.out.println("Latência: " + (endTime - startTime) / 1000 + "µs\n");
        long latencyMicros = (endTime - startTime) / 1000;

        metrics.MetricsRegistry.recordLatency(latencyMicros);
        metrics.MetricsRegistry.totalTrades.add(trades.size());

        return trades;
    }

    public boolean cancelOrder(long orderId){
        Order order = book.getOrder(orderId);
        if(order == null){
            return false;   
        }

        //salva o topo do livro antes do cancelamento
        long oldBestBid = getBestReferencePrice(book.bids);
        long oldBestAsk = getBestReferencePrice(book.asks);

        var sideMap = (order.side == Side.BUY) ? book.bids : book.asks;  
        PriceLevelQueue ordersAtPrice = sideMap.get(order.price);   

        if(ordersAtPrice != null){
            ordersAtPrice.remove(order);  
            if(ordersAtPrice.isEmpty()){   
                sideMap.remove(order.price);   
            }
        }

        book.removeOrderFromId(orderId); 
        
        //passa order.type
        dbWorker.enqueueOrder((byte) 3, order.id, order.price, order.getInitialQuantity(), order.getQuantity(), order.side == Side.BUY, order.type);
        
        //ordem foi removida do livro, então volta para a pool
        returnOrder(order);

        //salva o topo do livro depois do cancelamento
        long newBestBid = getBestReferencePrice(book.bids);
        long newBestAsk = getBestReferencePrice(book.asks);

        if(newBestBid != oldBestBid && newBestBid != 0){
            updatePeggedOrders(peggedBids, newBestBid, Side.BUY);
        }
        if(newBestAsk != oldBestAsk && newBestAsk != 0){
            updatePeggedOrders(peggedAsks, newBestAsk, Side.SELL);
        }
        
        return true;
    } 

    //encontra o topo do livro ignorando ordens pegged (busca preço real de referência)
    private long getBestReferencePrice(TreeMap<Long, PriceLevelQueue> sideMap){
        for(java.util.Map.Entry<Long, PriceLevelQueue> entry : sideMap.entrySet()){  //itera sobre níveis de preço do livro
            PriceLevelQueue queue = entry.getValue();                                //obtém fila de ordens naquele nível de preço
            Order current = queue.peekFirst();  
            while (current != null){
                if(current.type == enums.OrderType.LIMIT){  //verifica se a ordem atual é limit (liquidez real)
                    return entry.getKey();  
                }
                current = current.next;   //passa para a ordem seguinte
            }
        }
        return 0; //nenhuma ordem limit no livro
    }

    private void match(Order incoming, TreeMap<Long, PriceLevelQueue> oppositeSide, List<Trade> trades){
        while(!oppositeSide.isEmpty() && incoming.getQuantity() > 0){
            long bestOppositePrice = oppositeSide.firstKey();
            boolean canMatch = (incoming.type == OrderType.MARKET) || (incoming.side == Side.BUY ?
                                                                       incoming.price >= bestOppositePrice : 
                                                                       incoming.price <= bestOppositePrice);  

            if(!canMatch){
                break; 
            }

            PriceLevelQueue ordersAtLevel = oppositeSide.get(bestOppositePrice);   

            while(!ordersAtLevel.isEmpty() && incoming.getQuantity() > 0){
                Order restingOrder = ordersAtLevel.peekFirst();  

                //redundância de segurança: se uma ordem vazia escapou para o livro
                if(restingOrder.getQuantity() <= 0){    
                    ordersAtLevel.removeFirst();
                    book.removeOrderFromId(restingOrder.id);
                    returnOrder(restingOrder);   //reciclagem 
                    continue;
                }

                int matchQuantity = Math.min(incoming.getQuantity(), restingOrder.getQuantity()); 

                Trade trade = createTrade(incoming, restingOrder, matchQuantity, bestOppositePrice);    
                trades.add(trade);

                System.out.println("Trade, price: " + trade.price / 100.0 + ", qty: " + trade.quantity);  
                
                dbWorker.enqueueTrade((byte) 2, trade.buyerId, trade.sellerId, trade.price, trade.quantity); 

                incoming.reduceQuantity(matchQuantity);     
                restingOrder.reduceQuantity(matchQuantity); 

                //passa restingOrder.type
                dbWorker.enqueueOrder((byte) 1, restingOrder.id, restingOrder.price, restingOrder.getInitialQuantity(), restingOrder.getQuantity(), restingOrder.side == Side.BUY, restingOrder.type);
                
                if(restingOrder.getQuantity() == 0){
                    ordersAtLevel.removeFirst();
                    book.removeOrderFromId(restingOrder.id);  
                    
                    //a ordem do Maker que estava no livro foi totalmente preenchida, então volta para a piscina
                    returnOrder(restingOrder);
                }
            }
            if(ordersAtLevel.isEmpty()){
                oppositeSide.remove(bestOppositePrice);  
            }
        }
    }

    private Trade createTrade(Order incoming, Order resting, int quantity, long price){
        long buyerId = (incoming.side == Side.BUY) ? incoming.id : resting.id;
        long sellerId = (incoming.side == Side.SELL) ? incoming.id : resting.id;

        return new Trade(buyerId, sellerId, quantity, price);
    }

    public void printOrderBook(){
        this.book.printOrderBook();
    }

    public boolean modifyOrder(long oldOrderId, long newOrderId, long newPrice, int newQuantity){
        Order originalOrder = book.getOrder(oldOrderId);   
        if (originalOrder == null){
            return false;  
        }

        if(originalOrder.type == enums.OrderType.PEGGED){
            System.out.println("Aviso: Modificação de quantidade da ordem PEGGED aceita. O preço (" + newPrice / 100.0 + ") será ignorado e reatrelado automaticamente ao topo do livro.");
        }

        enums.Side originalSide = originalOrder.getSide();
        enums.OrderType originalType = originalOrder.getType();
        long originalPrice = originalOrder.price;   

        boolean canceled = cancelOrder(oldOrderId);  
        if(!canceled){
            return false;
        }

        long priceToSend = (originalType == enums.OrderType.PEGGED) ? originalPrice : newPrice;
        enqueue(newOrderId, priceToSend, newQuantity, newQuantity, originalSide, originalType);   

        return true;
    }  
}
