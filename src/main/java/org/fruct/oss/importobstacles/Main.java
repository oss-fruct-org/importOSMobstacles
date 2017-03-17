/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.fruct.oss.importobstacles;

import java.util.HashMap;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

/**
 *
 * @author kulakov
 */
public class Main {

    Connection c = null;

    /**
     * Почта суперадмина
     */
    String publicUser = "getsobstacletest@gmail.com";

    /**
     * Идентификатор суперадмина если найден, иначе 0
     */
    int publicUserID = 0;

    /**
     * Почта пользователя-владельца для импорта
     */
    String owner = "obstacle.osm@gmail.com";

    /**
     * Идентификатор пользователя-владельца или 0
     */
    int ownerID = 0;

    HashMap categories = null;

    public Main() throws ObjectNotFoundException {
        categories = new HashMap();

        // Соединение с БД
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/geo2tag",
                            "geo2tag", "geo2tag");
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
        Main m = new Main();

        //TODO: разбор файла с данными
        HashMap point = new HashMap();
        point.put("id", 365583834);
        point.put("type", "traffic_signals");
        point.put("latitude", 61.7789712);
        point.put("longitude", 34.3763235);
        point.put("horse", "no");
        point.put("bicycle", "yes");
        point.put("highway", "traffic_signals");
        point.put("crossing", "traffic_signals");

        if (m.importPoint(point)) {
            Logger.getLogger("logger").info("point " + point.get("id") + " was added/updated");
        } else {
            Logger.getLogger("logger").info("point " + point.get("id") + " was skipped");
        }
    }

    /**
     * Поиск точки в БД и загрузка/обновление при необходимости
     *
     * @param point Инфо о точке
     * @return true если точка была загружена/обновлена, иначе false
     */
    private boolean importPoint(HashMap point) throws SQLException, ObjectNotFoundException {
        if (!categories.containsKey(point.get("type"))) {
            findCategoryKey(point.get("type"));
            if (!categories.containsKey(point.get("type"))) {
                throw new ObjectNotFoundException("Category \"" + point.get("type") + "\" not found.");
            }
        }
        int category = (int) categories.get(point.get("type"));
        if (category == 0) {
            throw new ObjectNotFoundException("Category \"" + point.get("type") + "\" not found.");
        }

        Statement stmt = c.createStatement();

        // 1. ищем точку в БД среди публичных точек
        if (publicUserID > 0 && publicUserID != ownerID) {
            String query = "SELECT tag.id, tag.user_id, tag.label, tag.description from tag, subscribe, channel WHERE ABS(tag.latitude-" + point.get("latitude")
                    + ") < 0.00001 and ABS(tag.longitude-" + point.get("longitude")
                    + ") < 0.00001 and tag.channel_id=subscribe.channel_id and subscribe.user_id=" + publicUserID
                    + " and tag.channel_id=channel.id and channel.description like '%\"category_id\":\"" + category + "\"%';";
            //System.out.println(query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                int point_id = rs.getInt("id");
                int user_id = rs.getInt("user_id");
                String label = rs.getString("label");
                String description = rs.getString("description");
                stmt.close();
                if (description.contains("\"nodeID\":\"" + point.get("id").toString() + "\"")
                        || !description.contains("\"rating\"")) {
                    updatePoint(point, point_id, user_id, category, label, description);
                    return true;
                } else {
                    return false;
                }
            }
        }

        // 2. Поиск точки среди своих точек
        String query = "SELECT tag.id, tag.label, tag.description from tag, channel WHERE ABS(tag.latitude-" + point.get("latitude")
                + ") < 0.00001 and ABS(tag.longitude-" + point.get("longitude")
                + ") < 0.00001 and tag.user_id=" + ownerID + " tag.channel_id=channel.id and channel.description like '%\"category_id\":\"" + category + "\"%';";

        /*TODO: реализовать поиск своих точек. Если не найдено, то добавление */
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        //stmt.close();
        //return true;
    }

    /**
     * Поиск id категории по ключу
     *
     * @param key ключ
     */
    private void findCategoryKey(Object key) throws SQLException {
        String query = "select id from category where description like '%\"osmkey\":\"" + key + "\"%'";
        Statement stmt = c.createStatement();
        ResultSet rs = stmt.executeQuery(query);

        while (rs.next()) {
            categories.put(key, rs.getInt("id"));
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
    private void updatePoint(HashMap point, int pointID, int userID, int category, String label, String description) throws SQLException {
        Statement stmt = c.createStatement();
        // Ищем наш канал для нужной категории
        String query = "select id from channel where owner_id=" + ownerID + " and channel.description like '%\"category_id\":\"" + category + "\"%';";
        ResultSet rs = stmt.executeQuery(query);

        int channel = 0;
        while (rs.next()) {
            channel = rs.getInt("id");
        }
        stmt.close();

        if (channel == 0) {
            channel = createChannel(category);
        }
        System.out.println("Found channel " + channel);

        query = "update tag set label=?, description=?, user_id=?, channel_id=? where id=?";
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

}
