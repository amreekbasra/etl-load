package com.example.etl.config;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import oracle.jms.AQjmsFactory; // Ensure this import is correct
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory; // Optional: for connection caching
import org.springframework.jms.connection.UserCredentialsConnectionFactoryAdapter; // Optional: if pool doesn't handle creds

import jakarta.jms.ConnectionFactory; // Use jakarta namespace
import jakarta.jms.Session; // Use jakarta namespace
import java.sql.SQLException;

/**
 * Configures the JMS ConnectionFactory for Oracle AQ and the JMS Listener container.
 */
@Configuration
@EnableJms // Enable detection of @JmsListener annotations
public class JmsConfig {

    private static final Logger log = LoggerFactory.getLogger(JmsConfig.class);

    /**
     * Creates a JMS ConnectionFactory for Oracle AQ using Oracle UCP.
     * UCP provides robust connection pooling suitable for AQ JMS.
     *
     * @param url      JDBC URL for the Oracle database hosting AQ.
     * @param username Username for connecting to the database/AQ.
     * @param password Password for connecting to the database/AQ.
     * @return A configured JMS ConnectionFactory.
     * @throws SQLException If there's an error configuring the UCP pool or AQ factory.
     */
    @Bean(name = "oracleAqConnectionFactory")
    public ConnectionFactory oracleAqConnectionFactory(
            @Value("${oracle.aq.jms.url}") String url,
            @Value("${oracle.aq.jms.username}") String username,
            @Value("${oracle.aq.jms.password}") String password) throws SQLException {

        log.info("Configuring Oracle AQ JMS ConnectionFactory using UCP...");

        // 1. Create a UCP Pool DataSource specifically for JMS connections
        PoolDataSource ucpDataSource = PoolDataSourceFactory.getPoolDataSource();
        ucpDataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource"); // Standard Oracle pooled DS
        ucpDataSource.setURL(url);
        ucpDataSource.setUser(username);
        ucpDataSource.setPassword(password);
        // Configure UCP pool properties (adjust as needed)
        ucpDataSource.setInitialPoolSize(2); // Start with a few connections ready
        ucpDataSource.setMinPoolSize(2);     // Minimum connections to keep
        ucpDataSource.setMaxPoolSize(5);     // Max connections for JMS listener concurrency
        ucpDataSource.setValidateConnectionOnBorrow(true); // Good practice
        // Add other relevant UCP settings if required

        log.debug("UCP Pool DataSource configured for AQ JMS.");

        // 2. Create AQjmsFactory using the UCP DataSource
        // This allows AQ JMS operations (enqueue/dequeue) to leverage the UCP pool.
        ConnectionFactory aqConnectionFactory = AQjmsFactory.getConnectionFactory(ucpDataSource);
        log.info("Oracle AQ JMS ConnectionFactory created successfully.");

        // Optional: Wrap with Spring's CachingConnectionFactory for performance
        // CachingConnectionFactory cachingFactory = new CachingConnectionFactory(aqConnectionFactory);
        // cachingFactory.setSessionCacheSize(10); // Cache sessions
        // return cachingFactory;

        return aqConnectionFactory;
    }


    /**
     * Configures the Spring JMS Listener Container Factory.
     * This factory is used by @JmsListener annotations to create message listeners.
     *
     * @param connectionFactory The configured Oracle AQ ConnectionFactory.
     * @return A DefaultJmsListenerContainerFactory instance.
     */
    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(
            @Qualifier("oracleAqConnectionFactory") ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);

        // CRITICAL: Choose Acknowledge Mode carefully.
        // Session.CLIENT_ACKNOWLEDGE: Your listener code *must* call message.acknowledge().
        //                           Gives most control, allows ack only after successful processing (e.g., job launch).
        // Session.AUTO_ACKNOWLEDGE: Message acknowledged automatically upon listener return (risky if processing fails after return).
        // Session.SESSION_TRANSACTED: Requires a transaction manager. Ack happens on transaction commit.
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        log.info("JMS Listener Container Factory configured with CLIENT_ACKNOWLEDGE mode.");

        // Optional: Set up transaction management if needed (e.g., receive + DB update in one tx)
        // factory.setSessionTransacted(true);
        // factory.setTransactionManager(yourPlatformTransactionManagerBean);

        // Set concurrency (min-max number of concurrent consumers)
        factory.setConcurrency("1-5"); // Example: 1 to 5 concurrent listeners

        // Optional: Configure error handling (e.g., backoff, redelivery attempts)
        // factory.setErrorHandler(t -> log.error("Error in JMS listener", t));
        // Configure BackOffManager, etc. for redelivery policies

        return factory;
    }
}
