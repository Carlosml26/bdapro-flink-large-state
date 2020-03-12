package org.dima.bdapro.jmx;

/**
 * MBean to export the metrics through JMX.
 * The metrics being exported are:
 * <ul>
 *     <li>Processing Time Latency</li>
 *     <li>Event Time Latency</li>
 *     <li>Number of Transactions</li>
 * </ul>
 */
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
