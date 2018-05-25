package com.sapient.retail.price.rollup.model;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author hitgupta
 *
 */
@JsonIgnoreProperties(value = "id")
public class ProductPrice {
	
	private String productId;
	private Double minPrice;
	private Double maxPrice;
	private String currency;
	private List<SKUPrice> skuprice;
	
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
	 * @return the minPrice
	 */
	public Double getMinPrice() {
		return minPrice;
	}
	/**
	 * @param minPrice the minPrice to set
	 */
	public void setMinPrice(Double minPrice) {
		this.minPrice = minPrice;
	}
	/**
	 * @return the maxPrice
	 */
	public Double getMaxPrice() {
		return maxPrice;
	}
	/**
	 * @param maxPrice the maxPrice to set
	 */
	public void setMaxPrice(Double maxPrice) {
		this.maxPrice = maxPrice;
	}
	/**
	 * @return the skuprice
	 */
	public List<SKUPrice> getSkuprice() {
		return skuprice;
	}
	/**
	 * @param skuprice the skuprice to set
	 */
	public void setSkuprice(List<SKUPrice> skuprice) {
		this.skuprice = skuprice;
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
