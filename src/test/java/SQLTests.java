import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.testng.Assert;
import org.testng.annotations.Test;


import java.sql.*;
import java.util.TimeZone;

import static org.testng.Assert.assertEquals;

public class SQLTests {
    Connection connection = null;
    Statement statement = null;
    String url1 = "jdbc:mysql://127.0.0.1:3306/?serverTimezone=" + TimeZone.getDefault().getID();
    String url2 = "jdbc:mysql://127.0.0.1:3306/DB1?serverTimezone=" + TimeZone.getDefault().getID();

    @Test
    public void createDB() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url1, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("CREATE DATABASE DB1");
                // System.out.println("DB1 is created");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Cannot connect");
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"createDB"})
    public void createSchema() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("CREATE TABLE Students ( " +
                        "id INT (20)," +
                        "Age INT (20), " +
                        "FirstName NVARCHAR(20), " +
                        "LastName NVARCHAR(20));");
                System.out.println("Student Table created");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"createSchema"})
    public void insertSchema() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("INSERT INTO students (id, Age, FirstName, LastName) "
                        + " VALUES ('1', '18', 'John', 'Malkivic')," +
                        " ('2', '25', 'Andrew', 'Varlicht')," +
                        " ('3', '59', 'Petro', 'Petrovich')," +
                        " ('4', '143', 'Olga', 'Olgovna');");
                System.out.println("Create new student successful");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"insertSchema"})
    public void selectSchemaAfterCreate() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM db1.students WHERE age = 59;");
                while (resultSet.next()) {
                    int age = resultSet.getInt("age");
                    String firstName = resultSet.getString("FirstName");
                    assertEquals(59, age);
                    assertEquals(firstName, "Petro");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"selectSchemaAfterCreate"})
    public void updateSchema() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("UPDATE db1.students SET FirstName = 'Vasya' WHERE id = 2;");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"updateSchema"})
    public void selectSchemaAfterUpdate() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM db1.students WHERE id = 2;");
                while (resultSet.next()) {
                    String FirstName = resultSet.getString("FirstName");
                    assertEquals("Vasya", FirstName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"selectSchemaAfterUpdate"})
    public void deleteSchemaRow() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("DELETE FROM db1.Students WHERE id = 4;");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"deleteSchemaRow"})
    public void selectSchemaAfterDelete() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT * FROM db1.Students WHERE id = 4;");
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    Assert.assertNotNull(id);
                    int age = resultSet.getInt("age");
                    Assert.assertNotNull(age);
                    String FirstName = resultSet.getString("FirstName");
                    Assert.assertNotNull(FirstName);
                    String LastName = resultSet.getString("LastName");
                    Assert.assertNotNull(LastName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"selectSchemaAfterDelete"})
    public void dropSchema() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("DROP TABLE db1.students");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods = {"dropSchema"})
    public void checkSchemaAfterDrop() throws Exception {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet tables = metaData.getTables(null, null, "students", null);
                if (tables.next()) {
                    Assert.fail("Table is still present");
                } else {
                    System.out.println("Table was Deleted successful");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(dependsOnMethods={"checkSchemaAfterDrop"})
    public void dropDB() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate("DROP DATABASE db1");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }

    @Test(expectedExceptions = NullPointerException.class, dependsOnMethods={"dropDB"})
    public void checkDBAfterDrop() throws SQLException {
        try {
            connection = DataBaseConnection.conections(url2, "sergey", "sergey1989");
            if (connection != null) {
                Assert.fail("DB still exist");
            } else{
                System.out.println("DB was Deleted successful");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }
}



