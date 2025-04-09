package db.duck.dev.readcsv.config;

import javax.sql.DataSource;

import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Configuration
public class DuckDbConfig {

  @Bean
  public DataSource duckDbDataSource() {
    DriverManagerDataSource ds = new DriverManagerDataSource();
    ds.setDriverClassName("org.duckdb.DuckDBDriver");
    ds.setUrl("jdbc:duckdb:/tmp/duck.db");
    return ds;
  }

  @Bean
  public DefaultDSLContext duckDbDslContext(@Qualifier("duckDbDataSource") DataSource dataSource) {
    return new DefaultDSLContext(new DefaultConfiguration().set(dataSource).set(SQLDialect.DUCKDB));
  }
}
