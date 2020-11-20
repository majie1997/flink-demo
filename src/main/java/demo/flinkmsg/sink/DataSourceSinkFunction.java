package demo.flinkmsg.sink;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import demo.flinkmsg.entity.Trade;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.List;

//TODO, WIP
@Component
public class DataSourceSinkFunction extends RichSinkFunction<List<Trade>> implements DisposableBean {
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

    private HikariDataSource dataSource;
    private JdbcTemplate jdbcTemplate;

    private final String INSERT = "insert into trade(tradeId,tradeVersion,cusip,amount,price,createDate, processDate) values(?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = new HikariDataSource(createHikariConfig());
        jdbcTemplate = new JdbcTemplate(dataSource);
        super.open(parameters);
    }

    @Override
    public void invoke(List<Trade> trades, Context context) throws Exception {
        jdbcTemplate.batchUpdate(INSERT, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Trade trade = trades.get(i);
                ps.setInt(1, trade.getTradeId());
                ps.setInt(2, trade.getTradeVersion());
                ps.setObject(3, trade.getCusip());
                ps.setDouble(4, trade.getAmount());
                ps.setDouble(5, trade.getPrice());
                ps.setTimestamp(6, new Timestamp(trade.getCreateDate().getTime()));
                ps.setTimestamp(7, new Timestamp(System.currentTimeMillis()));

            }

            @Override
            public int getBatchSize() {
                return trades.size();
            }
        });
    }

    @Override
    public void close() throws Exception {
        destroy();
        super.close();
    }

    @Override
    public void destroy() throws Exception {
        if (null != dataSource) {
            dataSource.close();
        }
    }

    private HikariConfig createHikariConfig() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(h2Property.getDriver());
        config.setJdbcUrl(h2Property.getUrl());
        config.setUsername(h2Property.getUsername());
        config.setPassword(h2Property.getPassword());

        return config;
    }

}
