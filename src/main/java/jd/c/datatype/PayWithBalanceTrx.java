package jd.c.datatype;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * To Flink, a POJO means: 1. public fields or private fields with accessor 2. has method 3. has
 * default constructor
 *
 * @author jucheng
 * @see {@link org.apache.flink.api.java.typeutils.TypeExtractor#analyzePojo}
 */
public class PayWithBalanceTrx {

  private int trxId;
  private String trxType;

  public PayWithBalanceTrx() {
  }

  public PayWithBalanceTrx(int trxId, String trxType) {
    this.trxId = trxId;
    this.trxType = trxType;
  }

  public int getTrxId() {
    return trxId;
  }

  public void setTrxId(int trxId) {
    this.trxId = trxId;
  }

  public String getTrxType() {
    return trxType;
  }

  public void setTrxType(String trxType) {
    this.trxType = trxType;
  }

  @Override
  public String toString() {
    return "PayWithBalanceTrx [trxId=" + trxId + ", trxType=" + trxType + "]";
  }
}
