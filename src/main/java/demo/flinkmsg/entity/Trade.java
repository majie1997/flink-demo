package demo.flinkmsg.entity;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

//@Entity
//@Table(name="Trade")
public class Trade implements Serializable {
//    @Id
//    @GeneratedValue(strategy=GenerationType.AUTO)
//    private Long id;
//    @Column(name="tradeId")
    private Integer tradeId;
//    @Column(name="tradeVersion")
    private int tradeVersion;
//    @Column(name="cusip")
    private String cusip;
//    @Column(name="amount")
    private Double amount;
//    @Column(name="price")
    private Double price;
//    @Column(name="createDate")
    private Date createDate;
//    @Column(name="processDate")
    private Date processDate;

//    public Long getId() {
//        return id;
//    }
//
//    public void setId(Long id) {
//        this.id = id;
//    }

    public Integer getTradeId() {
        return tradeId;
    }

    public void setTradeId(Integer tradeId) {
        this.tradeId = tradeId;
    }

    public int getTradeVersion() {
        return tradeVersion;
    }

    public void setTradeVersion(int tradeVersion) {
        this.tradeVersion = tradeVersion;
    }

    public String getCusip() {
        return cusip;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Date getCreateDate() {
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public Date getProcessDate() {
        return processDate;
    }

    public void setProcessDate(Date processDate) {
        this.processDate = processDate;
    }
}
