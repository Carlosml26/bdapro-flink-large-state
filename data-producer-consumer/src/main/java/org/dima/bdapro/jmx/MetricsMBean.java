package org.dima.bdapro.jmx;

public interface MetricsMBean {
    Double getProcessingTimeLatency();
    Double getEventTimeLatency();
    Integer getTotalNumTransactions();
    Integer getTotalNumberMessagesIn ();
    void setProcessingTimeLatency(Double processingTimeLatency);
    void setEventTimeLatency(Double eventTimeLatency);
    void setTotalNumTransactions(Integer totalNumTransactions);
    void incTotalNumTransactions();
    void addMessage();
}
