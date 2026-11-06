package database;
import model.Order;
import enums.Side;
import enums.OrderType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

public class OrderDAO {
    private Connection conn;
    private PreparedStatement saveStmt;
    private PreparedStatement updateStmt;
    // >>> NOVO: cancelamento vai no MESMO lote e na MESMA conexão dos outros comandos
    private PreparedStatement cancelStmt;
    // <<< NOVO

    public OrderDAO(){
        try{
            conn = DatabaseConfig.getConnection();
            conn.setAutoCommit(false);   //desabilita o commit automático, para que as transações sejam feitas manualmente

            //comando para salvar ordens
            saveStmt = conn.prepareStatement("""
                    INSERT INTO orders (id, price, initial_quantity, quantity, side, type, status)
                    VALUES (?, ?, ?, ?, ?, ?, 'OPEN')
                    """);
            
                    //comando para atualizar ordens
            updateStmt = conn.prepareStatement("""
                    UPDATE orders
                    SET price = ?,quantity = ?, status = ?
                    WHERE id = ?
                    """);

            // >>> NOVO: comando para cancelar ordens (grava também o saldo que sobrou)
            cancelStmt = conn.prepareStatement("""
                    UPDATE orders
                    SET quantity = ?, status = 'CANCELLED'
                    WHERE id = ?
                    """);
            // <<< NOVO
        } catch (SQLException e){
            System.err.println("DAO - Erro ao inicializar PreparedStatements: " + e.getMessage());
        }
    }

    //adiciona os dados à fila para gravação
    public void addSaveBatch(long id, long price, int initialQty, int qty, boolean side, OrderType type) throws SQLException{
        saveStmt.setLong(1, id);
        saveStmt.setLong(2, price);
        saveStmt.setInt(3, initialQty);
        saveStmt.setInt(4, qty);
        saveStmt.setString(5, side ? "BUY" : "SELL");
        saveStmt.setString(6, type.name());
        
        saveStmt.addBatch();
    }

    //adiciona os dados à fila para atualização
    public void addUpdateBatch(long id, long price, int qty) throws SQLException{
        updateStmt.setLong(1, price);
        updateStmt.setInt(2, qty);
        updateStmt.setString(3, qty == 0 ? "FILLED" : "PARTIAL");
        updateStmt.setLong(4, id);
        updateStmt.addBatch();
    }

    public void addCancelBatch(long id, int remainingQty) throws SQLException{
        cancelStmt.setInt(1, remainingQty);
        cancelStmt.setLong(2, id);
        cancelStmt.addBatch();
    }

    //executa no banco
    public void executeBatches(){
        try{
            saveStmt.executeBatch();
            updateStmt.executeBatch();
            cancelStmt.executeBatch();   
            conn.commit();
        } catch (SQLException e){
            System.err.println("DAO - Erro ao executar batches: " + e.getMessage());
        }
    }

    //busca ordens abertas no banco para reconstruir o livro na RAM
    public List<Order> findAllOpen(){
        List<Order> openOrders = new ArrayList<>();
        String sql = "SELECT * FROM orders WHERE status = 'OPEN' OR status = 'PARTIAL'";

        try(Connection conn = DatabaseConfig.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery()){

            while(rs.next()){
                Order order = new Order(
                    rs.getLong("id"),
                    rs.getLong("price"),
                    rs.getInt("initial_quantity"),
                    rs.getInt("quantity"),  //quantidade restante no banco
                    Side.valueOf(rs.getString("side")),
                    OrderType.valueOf(rs.getString("type"))
                );
                openOrders.add(order);
            }
        }
        catch (SQLException e){
            System.err.println("DAO - Erro ao buscar ordens abertas: " + e.getMessage());
            e.printStackTrace();
        }

        return openOrders;
    }
}
