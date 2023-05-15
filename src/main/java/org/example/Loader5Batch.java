package org.example;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class Loader5Batch {

  private static final int BATCH_SIZE = 1000;
  private static Connection con = null;
  private static PreparedStatement stmt = null;

  private static void openDB(Properties prop) {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (Exception e) {
      System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
      System.exit(1);
    }
    String url =
        "jdbc:postgresql://" + prop.getProperty("host") + ":" + prop.getProperty("port")
            + "/" + prop.getProperty("database");
    try {
      con = DriverManager.getConnection(url, prop);
      if (con != null) {
        System.out.println("Successfully connected to the database "
            + prop.getProperty("database") + " as " + prop.getProperty("user"));
        con.setAutoCommit(false);
      }
    } catch (SQLException e) {
      System.err.println("Database connection failed");
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

  private static void closeDB() {
    if (con != null) {
      try {
        if (stmt != null) {
          stmt.close();
        }
        con.close();
        con = null;
      } catch (Exception ignored) {
      }
    }
  }

  private static Properties loadDBUser() {
    Properties properties = new Properties();
    try {
      properties.load(
          new InputStreamReader(new FileInputStream("resources/dbUser.properties")));
      return properties;
    } catch (IOException e) {
      System.err.println("can not find db user file");
      throw new RuntimeException(e);
    }
  }

  private static List<String> loadTXTFile() {
    try {
      return Files.readAllLines(Path.of("resources/movies.txt"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void loadData(String line) {
    String[] lineData = line.split(";");
    if (con != null) {
      try {
        stmt.setInt(1, Integer.parseInt(lineData[0]));
        stmt.setString(2, lineData[1]);
        stmt.setString(3, lineData[2]);
        stmt.setInt(4, Integer.parseInt(lineData[3]));
        stmt.setInt(5, Integer.parseInt(lineData[4]));
        stmt.executeUpdate();
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static void clearDataInTable() {
    Statement stmt0;
    if (con != null) {
      try {
        stmt0 = con.createStatement();
        stmt0.executeUpdate("DROP TABLE IF EXISTS Secondary_Reply;\n" +
            "DROP TABLE IF EXISTS Reply;\n" +
            "DROP TABLE IF EXISTS Post_Likes;\n" +
            "DROP TABLE IF EXISTS Post_Shares;\n" +
            "DROP TABLE IF EXISTS Post_Followers;\n" +
            "DROP TABLE IF EXISTS Post_Favorites;\n" +
            "DROP TABLE IF EXISTS Post;\n" +
            "DROP TABLE IF EXISTS Author;\n" +
            "DROP TABLE IF EXISTS City;\n"
        );

        con.commit();
        String sql1 = "CREATE TABLE if not exists Author (" +
            "author_id TEXT PRIMARY KEY," +
            "author_name TEXT NOT NULL UNIQUE," +
            "author_reg_time TIMESTAMP NOT NULL," +
            "author_phone TEXT" +
            ")";
        stmt0.executeUpdate(sql1);
        con.commit();
        String sql2 = "CREATE TABLE if not exists City (" +
            "city_id SERIAL PRIMARY KEY," +
            "city_name TEXT NOT NULL," +
            "country_name TEXT NOT NULL," +
            "CONSTRAINT uc_city_country UNIQUE (city_name, country_name)" +
            ")";
        stmt0.executeUpdate(sql2);
        con.commit();
        String sql3 = "CREATE TABLE if not exists Post (" +
            "post_id SERIAL PRIMARY KEY," +
            "title TEXT NOT NULL," +
            "category TEXT[] NOT NULL," +
            "content TEXT NOT NULL," +
            "posting_time TIMESTAMP NOT NULL," +
            "author_id TEXT NOT NULL," +
            "city_id INTEGER NOT NULL," +
            "FOREIGN KEY (author_id) REFERENCES Author(author_id)," +
            "FOREIGN KEY (city_id) REFERENCES City(city_id)" +
            ")";
        stmt0.executeUpdate(sql3);
        con.commit();
        String sql4 = "CREATE TABLE if not exists Post_Followers (" +
            "follow_id TEXT    NOT NULL," +
            "author_id TEXT NOT NULL," +
            "PRIMARY KEY (follow_id, author_id)," +
            "FOREIGN KEY (follow_id) REFERENCES Author(author_id)," +
            "FOREIGN KEY (author_id) REFERENCES Author(author_id)" +
            ")";
        stmt0.executeUpdate(sql4);
        con.commit();
        String sql5 = "CREATE TABLE if not exists Post_Favorites (" +
            "post_id INTEGER NOT NULL," +
            "author_id TEXT NOT NULL," +
            "PRIMARY KEY (post_id, author_id)," +
            "FOREIGN KEY (post_id) REFERENCES Post(post_id)," +
            "FOREIGN KEY (author_id) REFERENCES Author(author_id)" +
            ")";
        stmt0.executeUpdate(sql5);
        con.commit();
        String sql6 = "CREATE TABLE if not exists Post_Shares (" +
            "post_id INTEGER NOT NULL," +
            "author_id TEXT NOT NULL," +
            "PRIMARY KEY (post_id, author_id)," +
            "FOREIGN KEY (post_id) REFERENCES Post(post_id)," +
            "FOREIGN KEY (author_id) REFERENCES Author(author_id)" +
            ")";
        stmt0.executeUpdate(sql6);
        con.commit();
        String sql7 = "CREATE TABLE  if not exists Post_Likes (" +
            "post_id INTEGER NOT NULL," +
            "author_id TEXT NOT NULL," +
            "PRIMARY KEY (post_id, author_id)," +
            "FOREIGN KEY (post_id) REFERENCES Post(post_id)," +
            "FOREIGN KEY (author_id) REFERENCES Author(author_id)" +
            ")";
        stmt0.executeUpdate(sql7);
        con.commit();
        String sql8 = "CREATE TABLE if not exists Reply (" +
            "reply_id SERIAL PRIMARY KEY," +
            "post_id INTEGER NOT NULL," +
            "reply_content TEXT NOT NULL," +
            "reply_stars INTEGER NOT NULL," +
            "reply_author_id TEXT NOT NULL," +
            "FOREIGN KEY (post_id) REFERENCES Post(post_id)," +
            "FOREIGN KEY (reply_author_id) REFERENCES Author(author_id)," +
            " CONSTRAINT rep UNIQUE (post_id, reply_content, reply_stars, reply_author_id)" +
            ")";
        stmt0.executeUpdate(sql8);
        con.commit();
        String sql9 = "CREATE TABLE if not exists Secondary_Reply (" +
            "secondary_reply_id SERIAL PRIMARY KEY," +
            "reply_id INTEGER NOT NULL," +
            "secondary_reply_content TEXT NOT NULL," +
            "secondary_reply_stars INTEGER NOT NULL," +
            "secondary_reply_author_id TEXT NOT NULL," +
            "FOREIGN KEY (reply_id) REFERENCES Reply(reply_id)," +
            "FOREIGN KEY (secondary_reply_author_id) REFERENCES Author(author_id)," +
            "CONSTRAINT rep_sec UNIQUE (reply_id,secondary_reply_content,secondary_reply_stars,secondary_reply_author_id)"
            +
            ")";
        stmt0.executeUpdate(sql9);
        con.commit();
        stmt0.close();
      } catch (SQLException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static int cnt = 0;

  public static void addCnt() {
    cnt++;
    if (cnt % 1000 == 0) {
      System.out.println("insert " + 1000 + " data successfully!");
    }
  }


  private static void dealAuthor(Post post) throws SQLException {
    if (post.getAuthor() != null) {
      String sql = "INSERT INTO Author (author_id,author_name, author_reg_time, author_phone) VALUES (?, ?, ? ,?) ON CONFLICT (author_name) DO NOTHING";
      PreparedStatement statement = con.prepareStatement(sql);
      statement.setString(1, post.getAuthorID());
      statement.setString(2, post.getAuthor());
      statement.setTimestamp(3, Timestamp.valueOf(post.getAuthorRegistrationTime()));
      statement.setString(4, post.getAuthorPhone());
      usedIds.add(post.getAuthorID());
      statement.addBatch();
      statement.executeBatch();
      addCnt();
      con.commit();
      statement.close();
    }
  }

  private static void dealList(Post post) throws SQLException {
    dealAuthorList(post.getAuthorFavorite(), post.getAuthorRegistrationTime());
    dealAuthorList(post.getAuthorFollowedBy(), post.getAuthorRegistrationTime());
    dealAuthorList(post.getAuthorShared(), post.getAuthorRegistrationTime());
    dealAuthorList(post.getAuthorLiked(), post.getAuthorRegistrationTime());
  }

  private static void dealAuthorList(List<String> list, String time)
      throws SQLException {
    String sql = "INSERT INTO Author (author_id,author_name, author_reg_time, author_phone) VALUES (?, ?, ? ,?) ON CONFLICT (author_name) DO NOTHING";
    PreparedStatement statement = con.prepareStatement(sql);
    for (String s : list) {
      Timestamp t = Timestamp.valueOf(GenerateTime(time));
      statement.setString(1, GenerateAuthorId(t));
      statement.setString(2, s);
      statement.setTimestamp(3, t);
      statement.setString(4, GeneratePhone());
      //statement.addBatch();
      statement.executeUpdate();
      con.commit();
      addCnt();
    }

    statement.close();
  }

  private static void dealAuthorReply(Replies reply) throws SQLException {
    String replyAuthor = reply.getReplyAuthor();
    String secondaryReplyAuthor = reply.getSecondaryReplyAuthor();

    // 查询 Author 表中是否已存在该作者
    String selectAuthorSql = "SELECT author_id FROM Author WHERE author_name = ?";
    PreparedStatement selectStatement = con.prepareStatement(selectAuthorSql);
    selectStatement.setString(1, replyAuthor);
    ResultSet rs1 = selectStatement.executeQuery();

    String authorID;
    if (rs1.next()) {
      authorID = rs1.getString("author_id");
    } else {
      // 若不存在则插入该作者
      String insertAuthorSql = "INSERT INTO Author (author_id, author_name, author_reg_time, author_phone) VALUES (?, ?, ?, ?)";
      PreparedStatement insertStatement = con.prepareStatement(insertAuthorSql);
      Timestamp t = GenerateTimeByStamp(getPostTime(reply.getPostID()));
      authorID = GenerateAuthorId(t);
      insertStatement.setString(1, authorID);
      insertStatement.setString(2, replyAuthor);
      insertStatement.setTimestamp(3, t);

      insertStatement.setString(4, GeneratePhone());
      insertStatement.executeUpdate();
      insertStatement.close();
      addCnt();
    }

    rs1.close();
    selectStatement.close();

    // 查询 Secondary_Author 表中是否已存在该作者
    String selectSecondaryAuthorSql = "SELECT author_id FROM Author WHERE author_name = ?";
    PreparedStatement selectSecondaryStatement = con.prepareStatement(selectSecondaryAuthorSql);
    selectSecondaryStatement.setString(1, secondaryReplyAuthor);
    ResultSet rs2 = selectSecondaryStatement.executeQuery();

    String secondaryAuthorID;
    if (rs2.next()) {
      secondaryAuthorID = rs2.getString("author_id");
    } else {
      // 若不存在则插入该作者
      String insertSecondaryAuthorSql = "INSERT INTO Author (author_id, author_name, author_reg_time, author_phone) VALUES (?, ?, ?, ?)";
      PreparedStatement insertSecondaryStatement = con.prepareStatement(insertSecondaryAuthorSql);
      Timestamp t = GenerateTimeByStamp((getPostTime(reply.getPostID())));
      secondaryAuthorID = GenerateAuthorId(t);
      insertSecondaryStatement.setString(1, secondaryAuthorID);
      insertSecondaryStatement.setString(2, secondaryReplyAuthor);
      insertSecondaryStatement.setTimestamp(3, t);
      insertSecondaryStatement.setString(4, GeneratePhone());
      insertSecondaryStatement.executeUpdate();
      insertSecondaryStatement.close();
      addCnt();
    }

    rs2.close();
    selectSecondaryStatement.close();
  }


  public static String GeneratePhone() {
    int length = 10;
    String digits = "0123456789";
    Random rand = new Random();
    StringBuilder sb = new StringBuilder(length + 1);
    sb.append(1);
    for (int i = 0; i < length; i++) {
      int index = rand.nextInt(digits.length());
      char randomChar = digits.charAt(index);
      sb.append(randomChar);
    }
    return sb.toString();
  }

  private static Set<String> usedIds = new HashSet<>(); // 定义一个存储已使用过 id 的 Set

  public static String GenerateAuthorId(Timestamp timestamp) {
    Random rand = new Random();

    // 省份码
    String[] provinceCodes = {"11", "12", "13", "14", "15", "21", "22", "23", "31", "32", "33",
        "34", "35", "36", "37", "41", "42", "43", "44", "45", "46", "50", "51", "52", "53", "54",
        "61", "62", "63", "64", "65"};
    String provinceCode = provinceCodes[rand.nextInt(provinceCodes.length)];

    // 地址码（后4位）
    String addressCode = String.format("%04d", rand.nextInt(10000));

    // 生日码
    long maxBirthdayTimestamp =
        timestamp.getTime() - 365L * 24L * 60L * 60L * 1000L * 18L; // 18 years before timestamp
    long birthdayTimestamp =
        rand.nextLong() % (timestamp.getTime() - maxBirthdayTimestamp) + maxBirthdayTimestamp;
    Instant birthdayInstant = Instant.ofEpochMilli(birthdayTimestamp);
    ZonedDateTime birthday = ZonedDateTime.ofInstant(birthdayInstant, ZoneId.systemDefault());
    int year = birthday.getYear();
    int month = birthday.getMonthValue();
    int day = birthday.getDayOfMonth();
    String birthdayCode = String.format("%04d%02d%02d", year, month, day);
    String newCode;
    do {
      // 顺序码
      int sequenceCode = rand.nextInt(999) + 1;
      String sequenceCodeString = String.format("%03d", sequenceCode);

      // 计算校验码
      String code = provinceCode + addressCode + birthdayCode + sequenceCodeString;
      int[] weights = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
      String[] checkCodes = {"1", "0", "X", "9", "8", "7", "6", "5", "4", "3", "2"};
      int sum = 0;
      for (int i = 0; i < code.length(); i++) {
        sum += Character.getNumericValue(code.charAt(i)) * weights[i];
      }
      int checkCodeIndex = sum % 11;
      String checkCode = checkCodes[checkCodeIndex];
      newCode = code + checkCode;
    } while (usedIds.contains(newCode));
    usedIds.add(newCode);
    return newCode;
  }


  public static String GenerateTime(String time) {
    LocalDateTime dateTime = LocalDateTime.parse(time,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    LocalDateTime randomDateTime = dateTime.minusSeconds(
        ThreadLocalRandom.current().nextLong(0, dateTime.toEpochSecond(ZoneOffset.UTC)));
    return randomDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }

  private static Timestamp GenerateTimeByStamp(Timestamp timestamp) {
    LocalDateTime localDateTime = timestamp.toLocalDateTime();
    LocalDateTime randomDateTime = localDateTime.minusSeconds(
        ThreadLocalRandom.current().nextLong(0, localDateTime.toEpochSecond(ZoneOffset.UTC)));
    return Timestamp.valueOf(randomDateTime);
  }


  private static void dealPost(Post post) throws SQLException {
    if (post != null) {
      String sql = "INSERT INTO Post (post_id, title, category, content, posting_time, author_id, city_id) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (post_id) DO NOTHING";

      PreparedStatement statement = con.prepareStatement(sql);
      statement.setInt(1, post.getPostID());
      statement.setString(2, post.getTitle());
      statement.setArray(3, con.createArrayOf("text", post.getCategory().toArray()));
      statement.setString(4, post.getContent());
      statement.setTimestamp(5, Timestamp.valueOf(post.getPostingTime()));
      statement.setString(6, post.getAuthorID());

      // get city_id from City table
      String[] parts = post.getPostingCity().split(", ");
      StringBuilder city_name = new StringBuilder(parts[0]);
      for (int i = 1; i < parts.length - 1; i++) {
        city_name.append(",").append(parts[i]);
      }
      String country_name = parts[parts.length - 1];

      String getCityIDSQL = "SELECT city_id FROM City WHERE city_name = ? AND country_name = ?";
      PreparedStatement getCityIDStatement = con.prepareStatement(getCityIDSQL);
      getCityIDStatement.setString(1, String.valueOf(city_name));
      getCityIDStatement.setString(2, country_name);
      ResultSet resultSet = getCityIDStatement.executeQuery();
      if (resultSet.next()) {
        statement.setInt(7, resultSet.getInt("city_id"));
      } else {
        throw new SQLException("City not found in database");
      }

      statement.addBatch();
      statement.executeBatch();
      addCnt();
      con.commit();
      statement.close();
    }
  }


  private static void dealCity(Post post) throws SQLException {
    if (post.getPostingCity() != null) {
      String sql = "INSERT INTO City (city_name, country_name) VALUES (?, ?) ON CONFLICT (city_name, country_name) DO NOTHING";

      String[] parts = post.getPostingCity().split(", ");
      StringBuilder city_name = new StringBuilder(parts[0]);
      for (int i = 1; i < parts.length - 1; i++) {
        city_name.append(",").append(parts[i]);
      }
      String country_name = parts[parts.length - 1];
      PreparedStatement statement = con.prepareStatement(sql);
      statement.setString(1, String.valueOf(city_name));
      statement.setString(2, country_name);

      statement.addBatch();
      statement.executeBatch();
      addCnt();
      con.commit();
      statement.close();
    }
  }

  private static void dealPostFollowers(Post post) throws SQLException {
    int postID = post.getPostID();
    List<String> authorFollowedBy = post.getAuthorFollowedBy();
    String sql = "INSERT INTO Post_Followers (author_id,follow_id) VALUES (?, ?)";
    PreparedStatement statement = con.prepareStatement(sql);
    if (authorFollowedBy != null && !authorFollowedBy.isEmpty()) {
      for (String author : authorFollowedBy) {
        String authorID = getAuthorID(author);
        if (authorID != null) {
          statement.setString(1, post.getAuthorID());
          statement.setString(2, authorID);
          statement.executeUpdate();
          con.commit();
          addCnt();
        }
      }
      statement.close();
    }
  }

  private static void dealPostFavorites(Post post) throws SQLException {
    int postID = post.getPostID();
    List<String> authorFavorite = post.getAuthorFavorite();
    String sql = "INSERT INTO Post_Favorites (post_id, author_id) VALUES (?, ?)";
    PreparedStatement statement = con.prepareStatement(sql);
    if (authorFavorite != null && !authorFavorite.isEmpty()) {

      for (String author : authorFavorite) {
        String authorID = getAuthorID(author);
        if (authorID != null) {
          statement.setInt(1, postID);
          statement.setString(2, authorID);
          addCnt();
          statement.executeUpdate();
          con.commit();
        }
      }
      statement.close();
    }
  }

  private static void dealPostShares(Post post) throws SQLException {
    int postID = post.getPostID();
    List<String> authorShared = post.getAuthorShared();
    String sql = "INSERT INTO Post_Shares (post_id, author_id) VALUES (?, ?)";
    PreparedStatement statement = con.prepareStatement(sql);
    if (authorShared != null && !authorShared.isEmpty()) {
      for (String author : authorShared) {
        String authorID = getAuthorID(author);
        if (authorID != null) {
          statement.setInt(1, postID);
          statement.setString(2, authorID);
          statement.executeUpdate();
          con.commit();
          addCnt();
        }
      }
      statement.close();
    }
  }

  private static void dealPostLikes(Post post) throws SQLException {
    int postID = post.getPostID();
    List<String> authorLiked = post.getAuthorLiked();
    String sql = "INSERT INTO Post_Likes (post_id, author_id) VALUES (?, ?)";
    PreparedStatement statement = con.prepareStatement(sql);
    if (authorLiked != null && !authorLiked.isEmpty()) {

      for (String author : authorLiked) {
        String authorID = getAuthorID(author);
        if (authorID != null) {
          statement.setInt(1, postID);
          statement.setString(2, authorID);
          statement.executeUpdate();
          con.commit();
          addCnt();
        }
      }

      statement.close();
    }
  }

  private static String getAuthorID(String authorName) throws SQLException {
    String sql = "SELECT author_id FROM Author WHERE author_name = ?";
    PreparedStatement statement = con.prepareStatement(sql);
    statement.setString(1, authorName);
    ResultSet rs = statement.executeQuery();
    String authorID = null;
    if (rs.next()) {
      authorID = rs.getString("author_id");
    }
    rs.close();
    statement.close();
    return authorID;
  }

  private static Timestamp getPostTime(int Postid) throws SQLException {
    String sql = "SELECT posting_time FROM post WHERE post_id = ?";
    PreparedStatement statement = con.prepareStatement(sql);
    statement.setInt(1, Postid);
    ResultSet rs = statement.executeQuery();
    Timestamp time = null;
    if (rs.next()) {
      time = rs.getTimestamp("posting_time");
    }
    rs.close();
    statement.close();
    return time;
  }

  private static int getReplyId(Replies replies) throws SQLException {
    String selectReplySql = "SELECT reply_id FROM Reply WHERE post_id = ? AND reply_content = ? AND reply_stars = ? AND reply_author_id = ?";
    PreparedStatement statement = con.prepareStatement(selectReplySql);
    statement.setInt(1, replies.getPostID());
    statement.setString(2, replies.getReplyContent());
    statement.setInt(3, replies.getReplyStars());
    statement.setString(4, getAuthorID(replies.getReplyAuthor()));
    ResultSet rs = statement.executeQuery();
    rs.next();
    int replyID = rs.getInt(1);
    statement.close();
    rs.close();
    return replyID;
  }

  private static void dealReplies(Replies replies) throws SQLException {
    // 查询 Author ID
    String replyAuthorID = getAuthorID(replies.getReplyAuthor());
    String secondaryReplyAuthorID = getAuthorID(replies.getSecondaryReplyAuthor());

    // 向 Reply 表中插入数据
    String insertReplySql = "INSERT INTO Reply (post_id, reply_content, reply_stars, reply_author_id) VALUES (?, ?, ?, ?) ON CONFLICT (post_id, reply_content, reply_stars, reply_author_id) DO NOTHING";
    PreparedStatement statement1 = con.prepareStatement(insertReplySql);
    statement1.setInt(1, replies.getPostID());
    statement1.setString(2, replies.getReplyContent());
    statement1.setInt(3, replies.getReplyStars());
    statement1.setString(4, replyAuthorID);

    statement1.executeUpdate();
    addCnt();
    con.commit();
    statement1.close();

    int replyID = getReplyId(replies);

    // 向 Secondary_Reply 表中插入数据
    String insertSecondaryReplySql = "INSERT INTO Secondary_Reply (reply_id, secondary_reply_content, secondary_reply_stars, secondary_reply_author_id) VALUES (?, ?, ?, ?) ON CONFLICT (reply_id, secondary_reply_content, secondary_reply_stars, secondary_reply_author_id) DO NOTHING";
    PreparedStatement statement2 = con.prepareStatement(insertSecondaryReplySql);
    statement2.setInt(1, replyID);
    statement2.setString(2, replies.getSecondaryReplyContent());
    statement2.setInt(3, replies.getSecondaryReplyStars());
    statement2.setString(4, secondaryReplyAuthorID);

    statement2.executeUpdate();
    addCnt();
    con.commit();
    statement2.close();
  }

  public static void main(String[] args) {

    Properties prop = loadDBUser();

    // Empty target table
    openDB(prop);
    clearDataInTable();
    closeDB();

    long start = System.currentTimeMillis();
    openDB(prop);
    //Todo: 插入数据
    try {
      String jsonStrings = Files.readString(Path.of("src/project data and scripts/posts.json"));
      Type postListType = new TypeToken<List<Post>>() {
      }.getType();
      List<Post> posts = new Gson().fromJson(jsonStrings, postListType);

      String jsonStrings2 = Files.readString(Path.of("src/project data and scripts/replies.json"));
      Type replyListType = new TypeToken<List<Replies>>() {
      }.getType();
      List<Replies> replies = new Gson().fromJson(jsonStrings2, replyListType);

      for (Post post : posts) {
        dealCity(post);//从post中导入city
        dealAuthor(post);//导入author
      }
      for (Post post : posts) {
        dealList(post);
      }
      for (Post post : posts) {
        dealPost(post);
        dealPostFavorites(post);
        dealPostFollowers(post);
        dealPostLikes(post);
        dealPostShares(post);
      }
      for (Replies reply : replies) {
        dealAuthorReply(reply);
      }
      for (Replies reply : replies) {
        dealReplies(reply);
      }

    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
    closeDB();
    long end = System.currentTimeMillis();
    System.out.println(cnt + " records successfully loaded");
    System.out.println("Total time: " + (end - start)  + " ms");
    System.out.println("Loading speed : " + (cnt * 1000L) / (end - start) + " records/s");

  }

}

