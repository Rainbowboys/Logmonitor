package com.nicae.bigdata.logmonitor.utils;

import com.nicae.bigdata.logmonitor.dao.LogMonitorDao;
import com.nicae.bigdata.logmonitor.domain.*;
import com.nicae.bigdata.logmonitor.mail.MailInfo;
import com.nicae.bigdata.logmonitor.mail.MessageSender;
import com.nicae.bigdata.logmonitor.sms.SMSBase;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Describe: 日志监控的核心类，包括了日志监控系统所有的核心处理。
 * Author:   Rainbow
 * Domain:   www.nicae.cn
 */
public class MonitorHandler {


    //定义一个map，其中appId为Key，以该appId下的所有rule为Value
    private static HashMap<String, List<Rule>> ruleMap;
    //定义一个map,其中appId为Key，以该appId下的所有user为Value
    private static HashMap<String, List<User>> userMap;
    //定义一个list，用来封装所有的用户信息
    private static List<User> userList;
    //定义一个list，用来封装所有的应用信息
    private static List<App> applist;
    //定时加载配置文件的标识
    private static boolean reloaded = false;
    //定时加载配置文件的标识
    private static long nextReload = 0l;


    static {
        load();
    }

    /**
     * 处理一条一直消息 ，将消息内容按一定规则进行切割，对应字段进行校验
     *
     * @param line 一条消息
     * @return
     */
    public static Message parser(String line) {

        // 1$$$$$error: Caused by: java.lang.NoClassDefFoundError: com/starit/gejie/dao/
        //以5个$进行分割
        String[] messageArr = line.split("\\$\\$\\$\\$\\$");
        if (StringUtils.isBlank(messageArr[0]) || StringUtils.isBlank(messageArr[1])) {
            return null;
        }
        //检测appID是否已经通过授权合法
        if (apppIdisValid(messageArr[0])) {
            Message message = new Message();
            message.setAppId(messageArr[0]);
            message.setLine(messageArr[1]);
            return message;
        }
        return null;
    }

    /**
     * 对日志进行规制判定，看看是否触发规则
     *
     * @param message
     * @return
     */

    public static boolean trigger(Message message) {
        //如果规则模型为空，需要初始化加载规则模型
        if (ruleMap == null) {
            load();
        }
        //从规则模型中获取当前appid配置的规则
        List<Rule> ruleListByAppId = ruleMap.get(message.getAppId());
        for (Rule rule : ruleListByAppId) {
            if (message.getLine().contains(rule.getKeyword())) {
                message.setKeyword(rule.getKeyword());
                message.setRuleId(rule.getId() + " ");
                return true;
            }
        }
        return false;

    }


    /**
     * 加载数据模型，主要是用户列表、应用管理表、组合规则模型、组合用户模型。
     */
    public static synchronized void load() {
        if (userList == null) {
            userList = loadUserList();
        }
        if (applist == null) {
            applist = loadAppList();
        }
        if (ruleMap == null) {
            ruleMap = loadRuleMap();
        }
        if (userMap == null) {
            userMap = loadUserMap();
        }
    }

    private static boolean apppIdisValid(String appid) {
        try {
            for (App app : applist) {
                if (app.getId() == Integer.valueOf(appid)) {
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }


    /**
     * 定时加载配置信息
     * 配合reloadDataModel模块一起使用。
     * 主要实现原理如下：
     * 1，获取分钟的数据值，当分钟数据是10的倍数，就会触发reloadDataModel方法，简称reload时间。
     * 2，reloadDataModel方式是线程安全的，在当前worker中只有一个线程能够操作。
     * 3，为了保证当前线程操作完毕之后，其他线程不再重复操作，设置了一个标识符reloaded。
     * 在非reload时间段时，reloaded一直被置为true；
     * 在reload时间段时，第一个线程进入reloadDataModel后，加载完毕之后会将reloaded置为false。
     */
    public static void scheduleLoad() {
        if (System.currentTimeMillis() == nextReload) {
            reloadDataModel();
        }

    }

    /**
     * 配置scheduleLoad重新加载底层数据模型。
     */
    /**
     * thread 4
     * thread 3
     * thread 2
     */
    private static synchronized void reloadDataModel() {
        /* thread 1 --> reloaded -->false ->>true*/
        /* thread 2 --> reloaded  -- true*/
         /* thread 3 --> reloaded  -- true*/
        if (reloaded) {
            userList = loadUserList();
            applist = loadAppList();
            ruleMap = loadRuleMap();
            userMap = loadUserMap();

        }

    }

    /**
     * 封装应用和用户映射的map
     *
     * @return
     */
    private static HashMap<String, List<User>> loadUserMap() {
        //以应用的appId为key，以应用的所有负责人的userList对象为value。
        HashMap<String, List<User>> userMap = new HashMap<String, List<User>>();
        for (App app : applist) {
            String userId = app.getUserId();
            List<User> userList = userMap.get(app.getId() + "");
            if (userList == null) {
                userList = new ArrayList<User>();
                userMap.put(app.getId() + "", userList);
            }
            String[] userIds = userId.split(",");
            for (String userid : userIds) {
                userList.add(queryUserByid(userid));
            }
            userMap.put(app.getId() + "", userList);
        }
        return userMap;
    }

    /**
     * 通过用户编号获取用户的JavaBean
     *
     * @param userid
     * @return
     */
    private static User queryUserByid(String userid) {
        for (User user : userList) {
            if (user.getId() == Integer.valueOf(userid)) {
                return user;
            }
        }
        return null;
    }

    /**
     * 封装用户 和校验规则映射表
     *
     * @return
     */
    private static HashMap<String, List<Rule>> loadRuleMap() {

        HashMap<String, List<Rule>> map = new HashMap<String, List<Rule>>();
        LogMonitorDao logMonitorDao = new LogMonitorDao();
        List<Rule> ruleList = logMonitorDao.getRuleList();
        for (Rule rule : ruleList) {
            //将代表rule的list转化成一个map，转化的逻辑是，
            // 从rule.getAppId作为map的key，然后将rule对象作为value传入map
            //Map<appId,ruleList>  一个appid的规则信息，保存在一个list中。
            List<Rule> ruleListByAppId = map.get(rule.getAppId() + "");
            if (ruleListByAppId == null) {
                ruleListByAppId = new ArrayList<Rule>();
                map.put(rule.getAppId() + "", ruleListByAppId);
            }
            ruleListByAppId.add(rule);
            map.put(rule.getAppId() + "", ruleListByAppId);
        }
        return map;
    }

    /**
     * 访问数据库加载所有有效的应用的列表
     *
     * @return
     */
    private static List<App> loadAppList() {
        return new LogMonitorDao().getAppList();
    }

    /**
     * 访问数据库加载所有有效的用户列表
     *
     * @return
     */
    private static List<User> loadUserList() {
        return new LogMonitorDao().getUserList();
    }

    public static void notifly(String appid, Message message) {

        List<User> userList = queryUserByAppId(appid);
        //给列表中用户发送邮件通知
        if (sendEmail(appid, userList, message)) {
            message.setIsEmail(1);
        }
        //发送短信
//        if (sendSMS(appid, userList, message)) {
//            message.setIsPhone(1);
//        }

    }

    private static boolean sendSMS(String appid, List<User> userList, Message message) {
        ArrayList<String> mobileList = new ArrayList<String>();
        for (User user : userList) {
            mobileList.add(user.getMobile());
        }
        for (App app : applist) {
            if (app.getId() == Integer.valueOf(appid.trim())) {
                message.setAppName(app.getName());
                break;
            }
        }
        String content = "系统【" + message.getAppName() + "】在 " + DateUtils.getDateTime() + " 触发规则 " + message.getRuleId() + ",关键字：" + message.getKeyword();
        return SMSBase.sendSms(listToStringFormat(mobileList), content);


    }

    private static String listToStringFormat(ArrayList<String> mobileList) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < mobileList.size(); i++) {
            if (i == mobileList.size() - 1) {
                stringBuilder.append(mobileList.get(i));
            } else {
                stringBuilder.append(mobileList.get(i)).append(",");
            }
        }
        return stringBuilder.toString();

    }

    private static List<User> queryUserByAppId(String appid) {
        return userMap.get(appid);
    }

    private static boolean sendEmail(String appid, List<User> userList, Message message) {
        ArrayList<String> reciver = new ArrayList<String>();
        for (User user : userList) {
            reciver.add(user.getEmail());
        }
        for (App app : applist) {
            if (app.getId() == Integer.valueOf(appid.trim())) {
                message.setAppName(app.getName());
                break;
            }
        }
        if (reciver.size() > 0) {
            String dateTime = DateUtils.getDateTime();
            String content = "系统【" + message.getAppName() + "】在 " + dateTime + " 触发规则 " + message.getRuleId() + " ，过滤关键字为：" + message.getKeyword() + "  错误内容：" + message.getLine();
            MailInfo mailInfo = new MailInfo("系统运行日志监控", content, reciver, null);
            return MessageSender.sendMail(mailInfo);
        }
        return false;
    }

    public static void save(Record record) {

        new LogMonitorDao().saveRecord(record);
    }
}
