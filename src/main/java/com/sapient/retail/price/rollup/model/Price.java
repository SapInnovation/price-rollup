package com.sapient.retail.price.rollup.model;

import java.util.List;

import com.sapient.retail.price.rollup.model.SKUPrice;

public class Price {

	private String productId;
	private String currency;
	private List<SKUPrice> skuPrice;
	/**
	 * @return the productID
	 */
	public String getProductId() {
		return productId;
	}
	/**
	 * @param productID the productID to set
	 */
	public void setProductId(String productId) {
		this.productId = productId;
	}
	/**
	 * @return the skuPrice
	 */
	public List<SKUPrice> getSkuPrice() {
		return skuPrice;
	}
	/**
	 * @param skuPrice the skuPrice to set
	 */
	public void setSkuPrice(List<SKUPrice> skuPrice) {
		this.skuPrice = skuPrice;
	}
	/**
	 * @return the currency
	 */
	public String getCurrency() {
		return currency;
	}
	/**
	 * @param currency the currency to set
	 */
	public void setCurrency(String currency) {
		this.currency = currency;
	}
	
	
}
