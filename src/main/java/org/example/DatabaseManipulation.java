package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.*;

public class DatabaseManipulation implements DataManipulation {

    private Connection con = null;
    private ResultSet resultSet;

    private String host = "localhost";
    private String dbname = "cs307";
    private String user = "postgres";
    private String pwd = "fzb20030117   ";
    private String port = "1720";


    @Override
    public void openDatasource() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    @Override
    public void closeDatasource() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int addOneMovie(String str) {
        int result = 0;
        String sql = "insert into movies (title, country,year_released,runtime) " +
                "values (?,?,?,?)";
        String[] movieInfo = str.split(";");
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setString(1, movieInfo[0]);
            preparedStatement.setString(2, movieInfo[1]);
            preparedStatement.setInt(3, Integer.parseInt(movieInfo[2]));
            preparedStatement.setInt(4, Integer.parseInt(movieInfo[3]));
            System.out.println(preparedStatement.toString());

            result = preparedStatement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public String allContinentNames() {
        StringBuilder sb = new StringBuilder();
        String sql = "select continent from countries group by continent";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent")).append("\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    @Override
    public String continentsWithCountryCount() {
        StringBuilder sb = new StringBuilder();
        String sql = "select continent, count(*) countryNumber from countries group by continent;";
        try {
            Statement statement = con.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                sb.append(resultSet.getString("continent")).append("\t");
                sb.append(resultSet.getString("countryNumber"));
                sb.append(System.lineSeparator());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    @Override
    public String FullInformationOfMoviesRuntime(int min, int max) {
        StringBuilder sb = new StringBuilder();
        String sql = "select m.title,c.country_name country,c.continent ,m.runtime " +
                "from movies m " +
                "join countries c on m.country=c.country_code " +
                "where m.runtime between ? and ? order by runtime;";
        try {
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.setInt(1, min);
            preparedStatement.setInt(2, max);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                sb.append(resultSet.getString("runtime")).append("\t");
                sb.append(String.format("%-18s", resultSet.getString("country")));
                sb.append(resultSet.getString("continent")).append("\t");
                sb.append(resultSet.getString("title")).append("\t");
                sb.append(System.lineSeparator());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    @Override
    public String findMovieById(int id) {
        StringBuilder sb = new StringBuilder();
        String sql = "select m.title, c.country_name, m.year_released, m.runtime from movies m\n"
                + " join countries c\n"
                + " on m.country = c.country_code\n"
                + "where m.movieid = ? ;";
        try {
            PreparedStatement statement = con.prepareStatement(sql);
            //resultSet = statement.executeQuery(sql);
            statement.setInt(1,id);
            resultSet =statement.executeQuery();
            while (resultSet.next()) {
                sb.append("runtime:");
                sb.append(resultSet.getString("runtime")).append("\n");
                sb.append("country:");
                sb.append(resultSet.getString("country_name")).append("\n");
                sb.append("year released:");
                sb.append(resultSet.getString("year_released")).append("\n");
                sb.append("title:");
                sb.append(resultSet.getString("title")).append("\n");
                sb.append(System.lineSeparator());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

}
