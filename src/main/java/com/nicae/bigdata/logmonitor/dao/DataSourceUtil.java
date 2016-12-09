package com.nicae.bigdata.logmonitor.dao;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.log4j.Logger;

import javax.sql.DataSource;

/**
 * C3p0 连接池
 */
public class DataSourceUtil {
    private static Logger logger = Logger.getLogger(DataSourceUtil.class);
    private static DataSource dataSource;

    static {
        dataSource = new ComboPooledDataSource("logMonitor");
    }

    public static synchronized DataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new ComboPooledDataSource("logMonitor");
        }
        return dataSource;
    }

    public static void main(String[] args) {



    }


}
