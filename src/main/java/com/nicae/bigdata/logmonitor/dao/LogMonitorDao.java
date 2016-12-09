package com.nicae.bigdata.logmonitor.dao;

import com.nicae.bigdata.logmonitor.domain.App;
import com.nicae.bigdata.logmonitor.domain.Record;
import com.nicae.bigdata.logmonitor.domain.Rule;
import com.nicae.bigdata.logmonitor.domain.User;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Date;
import java.util.List;

/**
 * Created by Rainbow on 2016/12/8.
 */
public class LogMonitorDao {
    private static Logger logger = Logger.getLogger(LogMonitorDao.class);

    private JdbcTemplate jdbcTemplate;

    public LogMonitorDao() {
        this.jdbcTemplate = new JdbcTemplate(DataSourceUtil.getDataSource());
    }

    public List<Rule> getRuleList() {
        String sql = "SELECT `id`,`name`,`keyword`,`isValid`,`appId` FROM `log_monitor`.`log_monitor_rule` WHERE isValid=1";
        List<Rule> ruleList = jdbcTemplate.query(sql, new BeanPropertyRowMapper<Rule>(Rule.class));
        return ruleList;
    }

    public List<User> getUserList() {
        String sql = "SELECT `id`,`name`,`mobile`,`email`,`isValid` FROM `log_monitor`.`log_monitor_user` WHERE isValid =1";
        List<User> userList = jdbcTemplate.query(sql, new BeanPropertyRowMapper<User>(User.class));
        return userList;
    }

    public List<App> getAppList() {
        String sql = "SELECT `id`,`name`,`isOnline`,`typeId`,`userId`  FROM `log_monitor`.`log_monitor_app` WHERE isOnline =1";
        List<App> appList = jdbcTemplate.query(sql, new BeanPropertyRowMapper<App>(App.class));
        return appList;
    }

    /**
     * 将触发规则的消息记录保存到数据库中
     * @param record
     */
    public void saveRecord(Record record) {
        String sql = "INSERT INTO `log_monitor`.`log_monitor_rule_record`" +
                " (`appId`,`ruleId`,`isEmail`,`isPhone`,`isColse`,`noticeInfo`,`updataDate`) " +
                "VALUES ( ?,?,?,?,?,?,?)";

        jdbcTemplate.update(sql,record.getAppId(),record.getRuleId(),record.getIsEmail(),record.getIsPhone(),0,record.getLine(),new Date());

    }

    //测试连接池是否正常
    public static void main(String[] args) {
        LogMonitorDao logMonitorDao = new LogMonitorDao();
        System.out.println("Start......");
        List<App> appLists = logMonitorDao.getAppList();

    }



}
