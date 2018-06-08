package com.sapient.retail.price.rollup.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.util.StringUtils;

import com.sapient.retail.price.rollup.model.Price;
import com.sapient.retail.price.rollup.model.ProductPrice;
import com.sapient.retail.price.rollup.model.SKUPrice;


@Configuration
@EnableKafka
@EnableKafkaStreams

public class PriceRollUpConfiguration {
	
	@Autowired private KafkaProperties kafkaProperties;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Value (value = "${kafka.intopic}")
	private String priceKafkaInTopic;
	
	@Value (value = "${kafka.outtopic}")
	private String priceKafkaOutTopic;
	
	@Value (value = "${kafka.server}")
	private List<String> priceKafkaServer;
	
	@Bean
	public KafkaAdmin admin() {
	    Map<String, Object> configs = new HashMap<>();
	    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
	            StringUtils.collectionToCommaDelimitedString(priceKafkaServer));
	    return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic topic1() {
	    return new NewTopic(priceKafkaInTopic, 1, (short) 1);
	}

	@Bean
	public NewTopic topic2() {
	    return new NewTopic(priceKafkaOutTopic, 1, (short) 1);
	}

	@SuppressWarnings( "resource")
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
		logger.info("Setting Kafka configuration : StreamsConfig ");
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "price-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, priceKafkaServer);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(JsonDeserializer.DEFAULT_KEY_TYPE, String.class);
        props.put(JsonDeserializer.DEFAULT_VALUE_TYPE, Price.class);
        return new StreamsConfig(props);
    }
	
    @Bean
    public KStream<String, Price> kStreamJson(StreamsBuilder builder) {
    	logger.info("Opening stream to roll up data");
    	KStream<String, Price> stream = builder.stream(priceKafkaInTopic, Consumed.with(Serdes.String(), new JsonSerde<>(Price.class)));
    	
    	KStream<String, ProductPrice> rolledUpPrice = stream.map(new PriceRollUpMapper());
    	
    	rolledUpPrice.to(priceKafkaOutTopic, Produced.with(Serdes.String(), new JsonSerde<>(ProductPrice.class)));
        
        return stream;
    }
    
    public static class PriceRollUpMapper implements KeyValueMapper<String, Price, KeyValue<String, ProductPrice>> {
    	private Logger logger = LoggerFactory.getLogger(this.getClass());
    	@Override
		public KeyValue<String, ProductPrice> apply(String key, Price price) {
    		logger.info("Applying roll up for productId :" + price.getProductId());
			ProductPrice productPrice = new ProductPrice();
			List<SKUPrice> skuPrice = price.getSkuPrice();
			Double minPrice = 0.0 ;
			Double maxPrice = 0.0 ;
			
			
			for(SKUPrice skuPricedata :  skuPrice)
			{
				if (minPrice.equals(0.0) && maxPrice.equals(0.0) ) {
					minPrice = skuPricedata.getPrice();
					maxPrice = skuPricedata.getPrice();
				}
				if (skuPricedata.getPrice() < minPrice )
					{
					minPrice = skuPricedata.getPrice();
					}
				if (skuPricedata.getPrice() > maxPrice)
					{
					maxPrice = skuPricedata.getPrice();
					}
				
			}
			productPrice.setMaxPrice(maxPrice);
			productPrice.setMinPrice(minPrice);
			productPrice.setProductId(price.getProductId());
			productPrice.setSkuprice(price.getSkuPrice());
			productPrice.setCurrency(price.getCurrency());
			logger.info("roll up price for productId :" + productPrice.getMinPrice());
			
			return new KeyValue<String, ProductPrice>(key, productPrice);
		}

    }
}