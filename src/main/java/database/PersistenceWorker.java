package database;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import enums.OrderType;

public class PersistenceWorker implements Runnable{   //thread de persistência para gravar ordens e trades no banco de dados
    private final OrderDAO orderDAO = new OrderDAO();
    private final TradeDAO tradeDAO = new TradeDAO();

    //arrays da order
    private final long[] dbIds = new long[1024];
    private final long[] dbPrices = new long[1024];
    private final int[] dbInitialQuantity = new int[1024];
    private final int[] dbQuantity = new int[1024];
    private final boolean[] dbSide = new boolean[1024];
    private final OrderType[] dbType = new OrderType[1024]; 
    
    //arrays da trade
    private final long[] tradeBuyerIds = new long[1024];
    private final long[] tradeSellerIds = new long[1024];

    //guardam os tipos de tarefa a serem executadas
    private final byte[] dbTaskType = new byte[1024];
    private final AtomicIntegerArray ready = new AtomicIntegerArray(1024); 

    private final AtomicLong producer = new AtomicLong();
    private final AtomicLong consumer = new AtomicLong();

    //ring buffer para gravar ordens e trades no banco de dados, evitando que a thread principal seja bloqueada
    public void enqueueOrder(byte taskType, long id, long price, int initialQuantity, int quantity, boolean side, OrderType type){
        long currentProducer = producer.getAndIncrement();

        while(true){
            long currentConsumer = consumer.get();

            if(currentProducer - currentConsumer < 1024){
                int index = (int)(currentProducer & 1023);
                dbTaskType[index] = taskType;       //SAVE_ORDER ou UPDATE_ORDER

                dbIds[index] = id;
                dbPrices[index] = price;
                dbInitialQuantity[index] = initialQuantity;
                dbQuantity[index] = quantity;
                dbSide[index] = side;
                dbType[index] = type;

                ready.set(index, 1);
                break;
            } else {
                Thread.yield();
            }
        }
    }
    
    //ring buffer para gravar trades no banco de dados (é o mesmo ring buffer usado para gravar ordens)
    public void enqueueTrade(byte taskType, long buyerId, long sellerId, long price, int quantity){
        long currentProducer = producer.getAndIncrement();

        while(true){
            long currentConsumer = consumer.get();

            if(currentProducer - currentConsumer < 1024){
                int index = (int)(currentProducer & 1023);
                dbTaskType[index] = taskType;       //SAVE_TRADE

                tradeBuyerIds[index] = buyerId;
                tradeSellerIds[index] = sellerId;
                dbPrices[index] = price;
                dbQuantity[index] = quantity;

                ready.set(index, 1);
                break;
            } else {
                Thread.yield();
            }
        }
    }

    //loop infinito que processa as ordens e trades enfileirados, gravando-os no banco de dados em lotes
    @Override
    public void run(){
        System.out.println("[DB-WORKER] - Thread de persistência iniciada.");

        final int BATCH_LIMIT = 512;  //trigger para executar os batches de gravação no banco de dados quando atingir esse limite (metade do tamanho do ring buffer)
        final long WAIT_TIME_NANOS = 1_000_000;  //1ms 
        while(true){
            long currentProducer = producer.get();
            long currentConsumer = consumer.get();
            if(currentConsumer < currentProducer){
                int batchCount = 0;

                while(currentConsumer < currentProducer){
                    int index = (int)(currentConsumer & (1023));

                    while(ready.get(index) == 0){
                        Thread.yield();
                    } 

                    byte task = dbTaskType[index];

                    //cada tipo de tarefa é processada de acordo com o tipo (gravação de ordem (0), atualização de ordem (1), gravação de trade (2) ou cancelamento de ordem (3))
                    try{
                        if (task == 0){
                            orderDAO.addSaveBatch(dbIds[index], dbPrices[index], dbInitialQuantity[index], dbQuantity[index], dbSide[index], dbType[index]);
                        } else if (task == 1){
                            orderDAO.addUpdateBatch(dbIds[index], dbPrices[index],dbQuantity[index]);
                        } else if (task == 2){
                            tradeDAO.addSaveBatch(tradeBuyerIds[index], tradeSellerIds[index], dbPrices[index], dbQuantity[index]);
                        } else if (task == 3){
                            orderDAO.addCancelBatch(dbIds[index], dbQuantity[index]);
                        }
                    } catch (Exception e){
                        System.err.println("Erro ao processar batch: " + e.getMessage());
                    }
                    ready.set(index,0);
                    
                    currentConsumer++;
                    batchCount++;

                    if(batchCount >= BATCH_LIMIT){
                        orderDAO.executeBatches();
                        tradeDAO.executeBatches();
                        batchCount = 0;  //zero o contador para o restante do lote
                    }
                }

                consumer.set(currentConsumer);

                if(batchCount > 0){  //salva no disco o que sobrou do lote, caso a rede pare de enviar ordens
                        orderDAO.executeBatches();
                        tradeDAO.executeBatches();
                }
            } else {
                LockSupport.parkNanos(WAIT_TIME_NANOS);  //não é uma lock, é um comando para adormecer somente a thread atual por um tempo WAIT_TIME_NANOS, para evitar o busy waiting
            }
        }   
    }
}
