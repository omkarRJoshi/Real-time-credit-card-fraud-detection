package com.geca.kafka;
public class KafkaSchema {
//	cc_num,first,last,trans_num,trans_date,
//	trans_time,unix_time,category,merchant,
//	amt,merch_lat,merch_long,is_fraud
	
	private String cc_num;
	private String first;
	private String last;
	private String trans_num;
	private String trans_date;
	private String trans_time;
	private String unix_time;
	private String category;
	private String merchant;
	private String amt;
	private String merch_lat;
	private String merch_long;
	private String is_fraud;
	public String getCc_num() {
		return cc_num;
	}
	public void setCc_num(String cc_num) {
		this.cc_num = cc_num;
	}
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	public String getLast() {
		return last;
	}
	public void setLast(String last) {
		this.last = last;
	}
	public String getTrans_num() {
		return trans_num;
	}
	public void setTrans_num(String trans_num) {
		this.trans_num = trans_num;
	}
	public String getTrans_date() {
		return trans_date;
	}
	public void setTrans_date(String trans_date) {
		this.trans_date = trans_date;
	}
	public String getTrans_time() {
		return trans_time;
	}
	public void setTrans_time(String trans_time) {
		this.trans_time = trans_time;
	}
	public String getUnix_time() {
		return unix_time;
	}
	public void setUnix_time(String unix_time) {
		this.unix_time = unix_time;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getMerchant() {
		return merchant;
	}
	public void setMerchant(String merchant) {
		this.merchant = merchant;
	}
	public String getAmt() {
		return amt;
	}
	public void setAmt(String amt) {
		this.amt = amt;
	}
	public String getMerch_lat() {
		return merch_lat;
	}
	public void setMerch_lat(String merch_lat) {
		this.merch_lat = merch_lat;
	}
	public String getMerch_long() {
		return merch_long;
	}
	public void setMerch_long(String merch_long) {
		this.merch_long = merch_long;
	}
	public String getIs_fraud() {
		return is_fraud;
	}
	public void setIs_fraud(String is_fraud) {
		this.is_fraud = is_fraud;
	}
//	@Override
//	public String toString() {
//		return "KafkaSchema [cc_num=" + cc_num + ", first=" + first + ", last=" + last + ", trans_num=" + trans_num
//				+ ", trans_date=" + trans_date + ", trans_time=" + trans_time + ", unix_time=" + unix_time
//				+ ", category=" + category + ", merchant=" + merchant + ", amt=" + amt + ", merch_lat=" + merch_lat
//				+ ", merch_long=" + merch_long + ", is_fraud=" + is_fraud + "]";
//	}
	
	
	
}
