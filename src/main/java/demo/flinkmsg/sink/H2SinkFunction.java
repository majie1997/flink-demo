package demo.flinkmsg.sink;

import demo.flinkmsg.entity.Trade;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Date;
import java.util.List;

@Component
public class H2SinkFunction extends RichSinkFunction<List<Trade>> {
//    @Value("${spring.datasource.url}")
//    private String url;
//    @Value("${spring.datasource.driverClassName}")
//    private String driver;
//    @Value("${spring.datasource.username}")
//    private String username;
//    @Value("${spring.datasource.password}")
//    private String password;

    @Autowired
    private H2Property h2Property;

    private Connection connection;
    private PreparedStatement preparedStatement;

    private final String INSERT = "insert into trade(tradeId,tradeVersion,cusip,amount,price,createDate, processDate) values(?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(h2Property.getDriver());
        connection = DriverManager.getConnection(h2Property.getUrl(), h2Property.getUsername(), h2Property.getPassword());
        preparedStatement = connection.prepareStatement(INSERT);
        super.open(parameters);
    }

    @Override
    public void invoke(List<Trade> trades, Context context) throws Exception {
        trades.forEach(trade -> {
            try {
                preparedStatement.setInt(1, trade.getTradeId());
                preparedStatement.setInt(2, trade.getTradeVersion());
                preparedStatement.setObject(3, trade.getCusip());
                preparedStatement.setDouble(4, trade.getAmount());
                preparedStatement.setDouble(5, trade.getPrice());
                preparedStatement.setTimestamp(6, new Timestamp(trade.getCreateDate().getTime()));
                preparedStatement.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
                preparedStatement.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        });
        preparedStatement.executeBatch();
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
//
//    public String getUrl() {
//        return url;
//    }
//
//    public void setUrl(String url) {
//        this.url = url;
//    }
//
//    public String getDriver() {
//        return driver;
//    }
//
//    public void setDriver(String driver) {
//        this.driver = driver;
//    }
//
//    public String getUsername() {
//        return username;
//    }
//
//    public void setUsername(String username) {
//        this.username = username;
//    }
//
//    public String getPassword() {
//        return password;
//    }
//
//    public void setPassword(String password) {
//        this.password = password;
//    }
}
