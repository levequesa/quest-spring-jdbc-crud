package com.wildcodeschool.wildandwizard.repository;

import com.wildcodeschool.wildandwizard.entity.School;
import com.wildcodeschool.wildandwizard.util.JdbcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SchoolRepository implements CrudDao<School> {

    private final static String DB_URL = "jdbc:mysql://localhost:3306/spring_jdbc_quest?serverTimezone=GMT";
    private final static String DB_USER = "h4rryp0tt3r";
    private final static String DB_PASSWORD = "Horcrux4life!";

    @Override
    public School save(School school) {

        Connection connection = null;
        PreparedStatement request = null;
        ResultSet generatedKey = null ;
        
        try {
			connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			request = connection.prepareStatement("INSERT INTO school (name,capacity,country) VALUES (?,?,?)", Statement.RETURN_GENERATED_KEYS );
			request.setString(1, school.getName());
			request.setLong(2, school.getCapacity());
			request.setString(3, school.getCountry());
			if (request.executeUpdate() != 1) {
				throw new SQLException("failed to insert data");
			}
			generatedKey = request.getGeneratedKeys();
			if (generatedKey.next()) {
				Long id = generatedKey.getLong(1);
				school.setId(id);
				return school;
			} else {
                throw new SQLException("failed to get inserted id");
            }
        } catch (SQLException e) {
			e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(generatedKey);
			JdbcUtils.closeStatement(request);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public School findById(Long id) {

        Connection connection = null;
        PreparedStatement request = null;
        ResultSet result = null;
        
        try {
			connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			request = connection.prepareStatement("select * from school where id=?;");
			request.setLong(1, id);
			result = request.executeQuery();
			if (result.next()) {
				Long capacity = result.getLong("capacity");
				String name = result.getString("name");
				String country = result.getString("country");
				School school = new School(id, name, capacity, country);
				return (school);
			 } 
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(result);
			JdbcUtils.closeStatement(request);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public List<School> findAll() {

        Connection connection = null;
        PreparedStatement request = null;
        ResultSet result= null;
        
        try {
			connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			request = connection.prepareStatement("SELECT * FROM school;");
			result = request.executeQuery();
			
			List<School> schools = new ArrayList<School>();
			while (result.next()) {
				Long id = result.getLong("id");
				String name = result.getString("name");
				Long capacity = result.getLong("capacity");
				String country = result.getString("country");
				schools.add(new School(id,name,capacity,country));
			}
			return schools;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			JdbcUtils.closeResultSet(result);
			JdbcUtils.closeStatement(request);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public School update(School school) {

        Connection connection = null;
        PreparedStatement request = null;
        try {
			connection = DriverManager.getConnection(DB_URL, DB_URL, DB_PASSWORD);
			request = connection.prepareStatement("UPDATE school SET name=?,capacity=?,country=? WHERE id=?;");
			request.setString(1, school.getName());
			request.setLong(2, school.getCapacity());
			request.setString(3, school.getCountry());
			request.setLong(4,school.getId());
			if (request.executeUpdate() !=1) {
				throw new SQLException("failed to update data");
			} 
			return school;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			JdbcUtils.closeStatement(request);
			JdbcUtils.closeConnection(connection);
		}
        return null;
    }

    @Override
    public void deleteById(Long id) {

        Connection connection = null;
        PreparedStatement request = null;
        try {
			connection =DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			request = connection.prepareStatement("DELETE FROM school WHERE id = ?;");
			request.setLong(1, id);
			if (request.executeUpdate()!=1) {
				throw new SQLException("failed to delete data");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			JdbcUtils.closeStatement(request);
			JdbcUtils.closeConnection(connection);
		}
    }
}
