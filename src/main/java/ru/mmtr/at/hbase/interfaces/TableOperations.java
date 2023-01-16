package ru.mmtr.at.hbase.interfaces;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.mmtr.at.hbase.connection.ConnectionFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

//todo DI connection
public class TableOperations {

    /****
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param columnFamilies описание columnFamily {@link ColumnFamilyDescriptorBuilder}
     *
     *
     */
    @SneakyThrows
    public static void createTable(Connection c,
                                   String tableName,
                                   List<ColumnFamilyDescriptor> columnFamilies) {

        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        tableBuilder.setColumnFamilies(columnFamilies);
        c.getAdmin().createTable(tableBuilder.build());
    }


    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param columnFamilies описание columnFamily {@link ColumnFamilyDescriptorBuilder}
     */

    @SneakyThrows
    public static void addColumnFamily(Connection c,
                                    String tableName,
                                    List<ColumnFamilyDescriptor> columnFamilies) {

        if (!tableExist(c, tableName))
            throw new RuntimeException("Table " + tableName + " not exist");

        Admin a = c.getAdmin();
        //@SneakyThrows don't work for forEach() :(
        for (ColumnFamilyDescriptor colFamDesc : columnFamilies) {
            a.addColumnFamily(TableName.valueOf(tableName), colFamDesc);
        }
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @return Лист с описанием всех columnFamily{@link ColumnFamilyDescriptor} в таблице
     */

    @SneakyThrows
    public static List<ColumnFamilyDescriptor> getTableColumnFamilies(Connection c, String tableName) {

        return Arrays.asList(c.getAdmin().getDescriptor(TableName.valueOf(tableName)).getColumnFamilies());
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @return булевое значение существует ли таблица
     */
    @SneakyThrows
    public static boolean tableExist(Connection c,
                                     String tableName) {
        return c.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param columnFamilyName имя семейства столбцов (Column Family)
     *
     * @return булевое значение существует ли имя семейства столбцов (Column Family) в таблице
     */
    public static boolean checkFamilyInTable(Connection c,
                                             String tableName,
                                             String columnFamilyName) {

        List<ColumnFamilyDescriptor> arr = getTableColumnFamilies(c, tableName);
        return arr.stream().anyMatch(colDesc -> colDesc.getNameAsString().equals(columnFamilyName));
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param saveColumnFamilies сохранить семейства столбцов (Column Family)?
     */
    public static void purgeTable(Connection c,
                                  String tableName,
                                  boolean saveColumnFamilies) {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        if (saveColumnFamilies) {
            columnFamilyDescriptors = getTableColumnFamilies(c, tableName);
        }
        deleteTable(c, tableName);
        createTable(c, tableName, columnFamilyDescriptors);
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     */

    @SneakyThrows
    public static void deleteTable(Connection c,
                                   String tableName) {
        c.getAdmin().deleteTable(TableName.valueOf(tableName));
    }
}
