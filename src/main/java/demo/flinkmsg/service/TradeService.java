package demo.flinkmsg.service;

import demo.flinkmsg.entity.Trade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TradeService {
    @Autowired
    private JdbcTemplate jdbcTemplate;


    public List<Trade> findAll() {
        List<Trade> trades = jdbcTemplate.query("select * from trade", new BeanPropertyRowMapper<Trade>(Trade.class));
        return trades;
    }
}
