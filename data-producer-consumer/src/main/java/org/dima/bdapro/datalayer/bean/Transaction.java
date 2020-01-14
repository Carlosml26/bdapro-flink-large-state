package org.dima.bdapro.datalayer.bean;

import java.io.Serializable;

public class Transaction implements Serializable {
	Long transactionTime;
	String transactionId;

	String senderId;
	String senderType;

	String receiverId;
	String receiverType;

//	String profileId;

	Double transactionAmount;
//	Double transactionFee;
//	Double transactionBonus;


	public Transaction() {
	}

	public Transaction(Long transactionTime, String transactionId, String senderId, String senderType, String receiverId, String receiverType, Double transactionAmount) {
		this.transactionTime = transactionTime;
		this.transactionId = transactionId;
		this.senderId = senderId;
		this.senderType = senderType;
		this.receiverId = receiverId;
		this.receiverType = receiverType;
		this.transactionAmount = transactionAmount;
	}

	public Long getTransactionTime() {
		return transactionTime;
	}

	public void setTransactionTime(Long transactionTime) {
		this.transactionTime = transactionTime;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getSenderId() {
		return senderId;
	}

	public void setSenderId(String senderId) {
		this.senderId = senderId;
	}

	public String getSenderType() {
		return senderType;
	}

	public void setSenderType(String senderType) {
		this.senderType = senderType;
	}

	public String getReceiverId() {
		return receiverId;
	}

	public void setReceiverId(String receiverId) {
		this.receiverId = receiverId;
	}

	public String getReceiverType() {
		return receiverType;
	}

	public void setReceiverType(String receiverType) {
		this.receiverType = receiverType;
	}


	public Double getTransactionAmount() {
		return transactionAmount;
	}

	public void setTransactionAmount(Double transactionAmount) {
		this.transactionAmount = transactionAmount;
	}
}
