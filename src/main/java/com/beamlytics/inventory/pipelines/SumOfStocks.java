package com.beamlytics.inventory.pipelines;

import com.beamlytics.inventory.businesslogic.core.options.RetailPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Map;

public class SumOfStocks extends DoFn<KV<Row, Long>, KV<Row, Long>> {

    PCollectionView<Map<String, String>> side_input_view = null;
    //HashMap<String,Long> availabilityMap = null;
    JedisPoolConfig poolConfig;
    JedisPool jedis_pool;
    public SumOfStocks(PCollectionView<Map<String, String>> side_input_view) {
        this.side_input_view = side_input_view;
    }

    @Setup
    public void setup() {

        this.poolConfig = new JedisPoolConfig();
        poolConfig.setMaxIdle(100);
        poolConfig.setFairness(true);
        poolConfig.setLifo(false);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMinIdle(20);
// Maximum wait time when the connections are used up
        //poolConfig.setMaxWaitMillis(3000);
// When an object is obtained from the connection pool, a ping check will be performed first. If the check fails, the object will be removed and destroyed.
        poolConfig.setTestOnBorrow(false);
// When a connection is returned, a check will be performed first. Once the check fails, the connection will be terminated.
        poolConfig.setTestOnReturn(false);
// Set the connection pool mode to “queue”
        poolConfig.setLifo(false);
// Set the minimum connections
        // poolConfig.setTimeBetweenEvictionRunsMillis(3000);


    }

    @StartBundle
    public void startBundle(PipelineOptions options) {
        String hostname = options.as(RetailPipelineOptions.class).getRedisHost();
        Integer port = options.as(RetailPipelineOptions.class).getRedisPort();
        String auth = options.as(RetailPipelineOptions.class).getRedisAuth();
        jedis_pool = new JedisPool(this.poolConfig, hostname, port, 2000, auth);
    }

    @ProcessElement
    public void processElement(@Element KV<Row, Long> inputElement, OutputReceiver<KV<Row, Long>> out, @Timestamp Instant instant, ProcessContext pc) {

        Map sideInput = pc.sideInput(side_input_view);


        String product_id = Integer.toString(inputElement.getKey().getValue("product_id"));
        String store_id = "";//Integer.toString(inputElement.getKey().getValue("store_id"));
        Integer store_id_int = inputElement.getKey().getValue("store_id");
        if (store_id_int != null)
        {
            store_id  = Integer.toString(store_id_int);
        }
        String keyPattern = store_id + ":" + product_id;
        Long availability_from_redis = null;
        Long availability_calculated = null;
        //read availability from Redis
        String result;
        boolean broken = false;
        Jedis jedis_Connection = null;
        try {

            jedis_Connection = jedis_pool.getResource();
            //System.out.println("got connection " + jedis_Connection.toString());
            result = jedis_Connection.get("availability:" + keyPattern);
            try {
                if (null != result) {
                    availability_from_redis = Long.parseLong(result);
                } else {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException nfe) {
                //we want to continue with availability = count
                availability_from_redis = null;
            }

        } catch (JedisException exception) {
            broken = true;

            if (jedis_pool != null) {
                jedis_pool.returnBrokenResource(jedis_Connection);
            }

            throw new JedisException("The connection is broken");
        } finally {

            if (jedis_pool != null) {
                jedis_pool.returnResource(jedis_Connection);
            }
        }


        String redis_value = (String) sideInput.get(keyPattern);

        if (redis_value != null) {
            String[] values = redis_value.split(":");
            String timestamp = values[0];
            Long count = Long.parseLong(values[1]);
            Long transaction_total = inputElement.getValue() == null ? 0L : inputElement.getValue();


            if (instant.compareTo(Instant.ofEpochMilli(Long.parseLong(timestamp))) > 0) {


                if (availability_from_redis != null) {
                    availability_calculated = availability_from_redis + transaction_total;
                } else {
                    availability_calculated = count + transaction_total;
                }

                //put availability in Redis
                try {

                    jedis_Connection = jedis_pool.getResource();
                    jedis_Connection.set("availability:" + keyPattern, Long.toString(availability_calculated));


                } catch (JedisException exception) {
                    broken = true;

                    if (jedis_pool != null) {
                        jedis_pool.returnBrokenResource(jedis_Connection);
                    }

                    throw new JedisException("The connection is broken");
                } finally {

                    if (jedis_pool != null) {
                        jedis_pool.returnResource(jedis_Connection);
                    }
                }


                //end put availability in Redis
                out.outputWithTimestamp(KV.of(inputElement.getKey(), availability_calculated), instant);
            } else {
                out.outputWithTimestamp(KV.of(inputElement.getKey(), count), instant);
            }
        }


    }
}

