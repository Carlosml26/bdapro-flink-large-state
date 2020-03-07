package org.dima.bdapro.jmx;

public interface MetricsMBean {
    Double getProcessingTimeLatency();
    Double getEventTimeLatency();
    Integer getTotalNumTransactions();
    public void setProcessingTimeLatency (Double processingTimeLatency);
    public void setEventTimeLatency (Double eventTimeLatency);
    public void setTotalNumTransactions (Integer totalNumTransactions);
    public void sumProcessingTimeLatency (Double processingTimeLatency);
    public void sumEventTimeLatency (Double eventTimeLatency);
    public void incTotalNumTransactions ();
}
