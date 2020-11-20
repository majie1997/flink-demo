package demo.flinkmsg.exception;

public class VersionOutOfOrderException extends RuntimeException{

    public VersionOutOfOrderException(String message) {
        super(message);
    }
}
