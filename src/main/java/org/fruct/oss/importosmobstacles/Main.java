/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fruct.oss.importosmobstacles;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author kulakov
 */
public class Main {

    /**
     * Удаление дубликата из БД
     *
     * @param pointID ID дубликата
     */
    private void removeDuplicate(int pointID) throws SQLException {
        String query = " delete from tag where id=?;";
        PreparedStatement stmt = c.prepareStatement(query);
        stmt.setInt(1, pointID);
        int rows = stmt.executeUpdate();
        if (rows != 1) {
            throw new SQLException("Cannot delete point with id=" + pointID);
        } else {
            System.out.println("Remove point with id=" + pointID);
        }
    }

    /**
     * Сравнение описаний точек
     *
     * @param desc1 Описание точку 1 в формате json
     * @param desc2 Описание точки 2 в формате json
     * @return
     */
    private boolean compareDescriptions(String desc1, String desc2) {
        HashMap data1 = new HashMap<String, Object>();
        HashMap data2 = new HashMap<String, Object>();
        data1 = this.gson.fromJson(desc1, data1.getClass());
        data2 = this.gson.fromJson(desc2, data2.getClass());
        if (data1 == null || data2 == null) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        data1.remove("uuid");
        data2.remove("uuid");
        boolean flag = true;
        for (Object key : data1.keySet()) {
            if (!data2.containsKey(key) || !data1.get(key).equals(data2.get(key))) {
                flag = false;
                break;
            }
        }
        if (flag) {
            for (Object key : data2.keySet()) {
                if (!data1.containsKey(key)) {
                    flag = false;
                    break;
                }
            }
        }
        return flag;
    }

    public static enum Status {
        ADDED, SKIPPED, UPDATED, MOVED
    };

    Connection c = null;

    /**
     * Почта суперадмина
     */
    String publicUser;

    /**
     * Идентификатор суперадмина если найден, иначе 0
     */
    int publicUserID = 0;

    /**
     * Почта пользователя-владельца для импорта
     */
    String owner;

    /**
     * Идентификатор пользователя-владельца или 0
     */
    int ownerID = 0;

    HashMap categories = null;
    HashMap categoryNames = null;

    Gson gson;

    public Main(String _owner, String superUser, String host, int port, String login, String password) throws ObjectNotFoundException {
        categories = new HashMap();
        categoryNames = new HashMap();
        gson = new Gson();
        publicUser = superUser;
        owner = _owner;

        // Соединение с БД
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://" + host + ":" + port + "/geo2tag",
                            login, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");

        try {
            Statement stmt;
            stmt = c.createStatement();
            // получение id админа 
            String query = "select id from users where email='" + this.publicUser + "';";
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                publicUserID = rs.getInt("id");
            }

            // получение id пользователя
            query = "select id from users where email='" + this.owner + "';";
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                ownerID = rs.getInt("id");
            }
            stmt.close();
            System.out.println("Found IDs: " + String.valueOf(publicUserID) + " " + String.valueOf(ownerID));
        } catch (SQLException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (ownerID == 0) {
            throw new ObjectNotFoundException("User " + owner + " not found in database");
        }
    }

    public static void main(String[] args) throws ObjectNotFoundException, SQLException {
        /*TODO:  ввод параметров (email'ы, файл с даннными */
        Options opts = new Options();
        Option opt = new Option("u", "user", true, "E-mail of owned user");
        opt.setRequired(false);
        opts.addOption(opt);

        opt = new Option("s", "superuser", true, "E-mail of project super user");
        opt.setRequired(false);
        opts.addOption(opt);

        opt = new Option("i", "input", true, "Input file with obstacles");
        opt.setRequired(true);
        opts.addOption(opt);

        opt = new Option("h", "host", true, "GeTS database server host (default localhost)");
        opt.setRequired(false);
        opts.addOption(opt);

        opt = new Option("p", "port", true, "GeTS database server port (default 5432)");
        opt.setRequired(false);
        opts.addOption(opt);

        opt = new Option("l", "login", true, "GeTS database login (default geo2tag)");
        opt.setRequired(false);
        opts.addOption(opt);

        opt = new Option("w", "password", true, "GeTS database password (default geo2tag)");
        opt.setRequired(false);
        opts.addOption(opt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException ex) {
            System.err.println(ex.getMessage());
            formatter.printHelp("importOSMobstacles", opts);
            System.exit(1);
            return;
        }

        String input_file = cmd.getOptionValue("input");
        String owner = cmd.getOptionValue("user", "obstacle.osm@gmail.com");
        String superUser = cmd.getOptionValue("superuser", "getsobstacletest@gmail.com");
        String host = cmd.getOptionValue("host", "localhost");
        int port = Integer.parseInt(cmd.getOptionValue("port", "5432"));
        String login = cmd.getOptionValue("login", "geo2tag");
        String password = cmd.getOptionValue("password", "geo2tag");

        Main m = new Main(owner, superUser, host, port, login, password);

        //TODO: разбор файла с данными
        long countPoints = 0;

        try {
            JsonReader reader = new JsonReader(new FileReader(input_file));
            reader.beginArray();
            HashMap point = new HashMap<String, Object>();
            while (reader.hasNext()) {
                point = m.gson.fromJson(reader, point.getClass());

                Main.Status ret = m.importPoint(point);

                switch (ret) {
                    case SKIPPED:
                        System.out.println("Point #" + countPoints + " ID=" + ((Double) point.get("id")).intValue() + " was skipped");
                        break;
                    case ADDED:
                        System.out.println("Point #" + countPoints + " ID=" + ((Double) point.get("id")).intValue() + " was added");
                        break;
                    case UPDATED:
                        System.out.println("Point #" + countPoints + " ID=" + ((Double) point.get("id")).intValue() + " was updated");
                        break;
                    case MOVED:
                        System.out.println("Point #" + countPoints + " ID=" + ((Double) point.get("id")).intValue() + " was moved");
                        break;
                }
                countPoints++;
            }

//        HashMap point = new HashMap();
//        point.put("id", 365583834);
//        point.put("type", "traffic_signals");
//        point.put("latitude", 61.7789712);
//        point.put("longitude", 34.3763235);
//        point.put("horse", "no");
//        point.put("bicycle", "yes");
//        point.put("highway", "traffic_signals");
//        point.put("crossing", "traffic_signals");
//        if (m.importPoint(point)) {
//            Logger.getLogger("logger").info("point " + point.get("id") + " was added/updated");
//        } else {
//            Logger.getLogger("logger").info("point " + point.get("id") + " was skipped");
//        }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Поиск точки в БД и загрузка/обновление при необходимости
     *
     * @param point Инфо о точке
     * @return true если точка была загружена/обновлена, иначе false
     */
    private Status importPoint(HashMap point) throws SQLException, ObjectNotFoundException {
        if (!categories.containsKey(point.get("type"))) {
            findCategoryByKey(point.get("type"));
            if (!categories.containsKey(point.get("type"))) {
                throw new ObjectNotFoundException("Category \"" + point.get("type") + "\" not found.");
            }
        }
        int category = (int) categories.get(point.get("type"));
        if (category == 0) {
            throw new ObjectNotFoundException("Category \"" + point.get("type") + "\" not found.");
        }

        Statement stmt = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // 1. Поиск точки среди своих точек
        String query = "SELECT tag.id, tag.label, tag.description from tag, channel WHERE ABS(tag.latitude-" + point.get("latitude")
                + ") < 0.00001 and ABS(tag.longitude-" + point.get("longitude")
                + ") < 0.00001 and tag.user_id=" + ownerID + " and tag.channel_id=channel.id and channel.description like '%\"category_id\":\"" + category + "\"%';";

        ResultSet rs = stmt.executeQuery(query);
        if (rs.last()) {
            int rows = rs.getRow();
            if (rows > 1) {
                //TODO: реализовать обработку нескольких результатов
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            int point_id = rs.getInt("id");
            String label = rs.getString("label");
            String description = rs.getString("description");

            if (validatePoint(point, label, description)) {
                return Status.SKIPPED;
            } else {
                updatePoint(point, point_id, ownerID, category, label, description);
                System.out.println(Thread.currentThread().getStackTrace()[1].getLineNumber() + ": Update my point ID=" + point_id);
                return Status.UPDATED;
            }
        }

        // 2. ищем точку в БД среди публичных не своих точек 
        if (publicUserID > 0 && publicUserID != ownerID) {
            query = "SELECT tag.id, tag.user_id, tag.label, tag.description from tag, subscribe, channel WHERE ABS(tag.latitude-" + point.get("latitude")
                    + ") < 0.00001 and ABS(tag.longitude-" + point.get("longitude")
                    + ") < 0.00001 and tag.channel_id=subscribe.channel_id and subscribe.user_id=" + publicUserID
                    + " and tag.channel_id=channel.id and tag.user_id <> " + ownerID + " and channel.description like '%\"category_id\":\"" + category + "\"%';";
            //System.out.println(query);
            rs = stmt.executeQuery(query);
            if (rs.last()) {
                int rows = rs.getRow();
                int point_id = rs.getInt("id");
                int user_id = rs.getInt("user_id");
                String label = rs.getString("label");
                String description = rs.getString("description");
                if (rows > 1) {
                    rs.beforeFirst();
                    while (rs.next() && !rs.isLast()) {
                        if (rs.getInt("user_id") == user_id
                                && rs.getString("label").equals(label)
                                && compareDescriptions(rs.getString("description"), description)) {
                            this.removeDuplicate(rs.getInt("id"));
                        } else {
                            throw new ObjectNotFoundException("duplicate points!\n first point: id=" + rs.getInt("id") + "; user_id=" + rs.getInt("user_id") + "; label=" + rs.getString("label") + "; desc=" + rs.getString("description")
                                    + ";\n second point: id=" + point_id + "; user=" + user_id + "; label=" + label + "; desc=" + description);
                        }
                    }
                }
                stmt.close();
                if (!validatePoint(point, label, description)) {
                    updatePoint(point, point_id, user_id, category, label, description);
                    return Status.MOVED;
                } else {
                    return Status.SKIPPED;
                }
            }
        }

        // 3. точка не найдена ни среди своих, ни среди публичных - добавляем
        stmt.close();
        if (addPoint(point, category)) {
            return Status.ADDED;
        } else {
            return Status.SKIPPED;
        }
    }

    /**
     * Поиск id категории по ключу
     *
     * @param key ключ
     */
    private void findCategoryByKey(Object key) throws SQLException, ObjectNotFoundException {
        String query = "select id,name from category where description like '%\"osmkey\":\"" + key + "\"%'";
        Statement stmt = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery(query);

        if (rs.last()) {
            if (rs.getRow() > 1) {
                throw new SQLException("Multiple categories with osmkey=" + key);
            }
            categories.put(key, rs.getInt("id"));
            categoryNames.put(rs.getInt("id"), rs.getString("name"));
        } else {
            throw new ObjectNotFoundException("Can't find category with osmkey=" + key);
        }
        System.out.println("Category " + key + " has ID=" + categories.get(key));
        stmt.close();
    }

    /**
     * Обновление информации о точке
     *
     * @param point Новая информация о точке
     * @param pointID Идентификатор точки
     * @param userID Идентификатор пользователя
     * @param label Название точки
     * @param description Описание точки
     */
    private void updatePoint(HashMap point, int pointID, int userID, int category, String label, String description) throws SQLException, ObjectNotFoundException {
        Statement stmt = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        // Ищем наш канал для нужной категории
        String query = "select id from channel where owner_id=" + ownerID + " and channel.description like '%\"category_id\":\"" + category + "\"%';";
        ResultSet rs = stmt.executeQuery(query);

        int channel = 0;
        if (rs.last()) {
            if (rs.getRow() > 1) {
                throw new ObjectNotFoundException("Can't find channel for category id=" + category);
            }
            channel = rs.getInt("id");
        }
        stmt.close();

        if (channel == 0) {
            channel = createChannel(category);
        }
        System.out.println("Found channel " + channel);

        //TODO: исправление содержания label и description
        Map<String, Object> descriptionMap = new HashMap<String, Object>();
        descriptionMap = (Map<String, Object>) gson.fromJson(description, descriptionMap.getClass());
        if (descriptionMap.containsKey("nodeID")) {
            descriptionMap.put("osmID", descriptionMap.get("nodeID"));
            descriptionMap.remove("nodeID");
        }
        if (!descriptionMap.containsKey("rating")) {
            descriptionMap.put("rating", 1);
        }
        description = gson.toJson(descriptionMap);
        //System.out.println("new description: " + description);

        query = "update tag set label=?, description=?, user_id=?, channel_id=? where id=?;";
        PreparedStatement pstmt = c.prepareStatement(query);
        pstmt.setString(1, label);
        pstmt.setString(2, description);
        pstmt.setInt(3, ownerID);
        pstmt.setInt(4, channel);
        pstmt.setInt(5, pointID);
        if (pstmt.executeUpdate() != 1) {
            throw new SQLException("Point ID=" + pointID + " not updated");
        }
    }

    private boolean addPoint(HashMap point, int category) throws SQLException, ObjectNotFoundException {
        Statement stmt = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        // Ищем наш канал для нужной категории
        String query = "select id from channel where owner_id=" + ownerID + " and channel.description like '%\"category_id\":\"" + category + "\"%';";
        ResultSet rs = stmt.executeQuery(query);

        int channel = 0;
        if (rs.last()) {
            if (rs.getRow() > 1) {
                throw new ObjectNotFoundException("Can't find channel for category id=" + category);
            }
            channel = rs.getInt("id");
        }
        stmt.close();

        if (channel == 0) {
            channel = createChannel(category);
        }
        System.out.println("addPoint(): Found channel " + channel);

        // добавляем точку
        HashMap descriptionMap = new HashMap();
        descriptionMap.put("osmID", ((Double) point.get("id")).intValue());
        descriptionMap.put("description", "Import from Openstreetmap service");
        descriptionMap.put("description_ru", "Загружено с сервиса Openstreetmap");
        descriptionMap.put("rating", 1);
        String description = gson.toJson(descriptionMap);

        query = "insert into tag (label, description, channel_id, user_id, latitude, longitude, altitude, url) values (?,?,?,?,?,?,?,?);";
        PreparedStatement pstmt = c.prepareStatement(query);
        pstmt.setString(1, (String) categoryNames.get(category));
        pstmt.setString(2, description);
        pstmt.setInt(3, channel);
        pstmt.setInt(4, ownerID);
        pstmt.setDouble(5, (double) point.get("latitude"));
        pstmt.setDouble(6, (double) point.get("longitude"));
        pstmt.setDouble(7, 0.0);
        pstmt.setString(8, "{}");
        //TODO: добавить нужные поля!!!!

        if (pstmt.executeUpdate() != 1) {
            throw new SQLException("Point ID=" + point.get("id") + " not inserted");
        }
        return true;
    }

    private int createChannel(int categoryID) throws SQLException {
        /*TODO: указывать название категории а не id */
        String query = "insert into channel (name, description, url, owner_id) values(?,?,?,?) returning channel.id;";
        PreparedStatement stmt = c.prepareStatement(query);
        stmt.setString(1, "ch+" + categoryID + "+" + ownerID);
        stmt.setString(2, "{\"description\":\"Private channel for category " + categoryID + " user " + owner + "\",\"category_id\":\"" + categoryID + "\"}");
        stmt.setString(3, "{}");
        stmt.setInt(4, ownerID);
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("Category " + categoryID + " not inserted");
    }

    /**
     * Проверка валидности точки (наличия всех полей корректно заполненных).
     * Сейчас проверяются устаревшее поле nodeID (должно быть osmID) и наличие
     * рейтинга
     *
     * @param point данные о точке из OSM
     * @param label название точки в GeTS
     * @param description описание точки в GeTS
     * @return истина если название и описание точки корректны, иначе ложь
     */
    private boolean validatePoint(HashMap point, String label, String description) {
        if (description.contains("\"nodeID\":\"" + point.get("id").toString() + "\"")
                || !description.contains("\"rating\"")) {
            return false;
        } else {
            return true;
        }
    }

}
