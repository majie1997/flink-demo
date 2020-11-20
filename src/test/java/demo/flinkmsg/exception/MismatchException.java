package demo.flinkmsg.exception;

import demo.flinkmsg.entity.Trade;
import demo.flinkmsg.entity.TradeMessage;

import java.util.List;

public class MismatchException extends RuntimeException{

    public MismatchException(String message) {
        super(message);
    }
}
