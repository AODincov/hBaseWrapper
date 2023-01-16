package ru.mmtr.at.hbase.interfaces;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.mmtr.at.hbase.connection.ConnectionFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class TableOperations {

    Connection connection;
    /****

     * @param tableName имя таблицы в БД
     * @param columnFamilies описание columnFamily {@link ColumnFamilyDescriptorBuilder}
     *
     *
     */
    @SneakyThrows
    public void createTable(String tableName,
                                   List<ColumnFamilyDescriptor> columnFamilies) {

        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        tableBuilder.setColumnFamilies(columnFamilies);
        connection.getAdmin().createTable(tableBuilder.build());
    }


    /***
     *
     * @param tableName имя таблицы в БД
     * @param columnFamilies описание columnFamily {@link ColumnFamilyDescriptorBuilder}
     */

    @SneakyThrows
    public void addColumnFamily(String tableName,
                                    List<ColumnFamilyDescriptor> columnFamilies) {

        if (!tableExist(tableName))
            throw new RuntimeException("Table " + tableName + " not exist");

        Admin a = connection.getAdmin();
        //@SneakyThrows don't work for forEach() :(
        for (ColumnFamilyDescriptor colFamDesc : columnFamilies) {
            a.addColumnFamily(TableName.valueOf(tableName), colFamDesc);
        }
    }

    /***
     *
     * @param tableName имя таблицы в БД
     * @return Лист с описанием всех columnFamily{@link ColumnFamilyDescriptor} в таблице
     */

    @SneakyThrows
    public List<ColumnFamilyDescriptor> getTableColumnFamilies(Connection connection, String tableName) {

        return Arrays.asList(connection.getAdmin().getDescriptor(TableName.valueOf(tableName)).getColumnFamilies());
    }

    /***
     *
     * @param tableName имя таблицы в БД
     * @return булевое значение существует ли таблица
     */
    @SneakyThrows
    public boolean tableExist(String tableName) {
        return connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    /***
     *
     * @param tableName имя таблицы в БД
     * @param columnFamilyName имя семейства столбцов (Column Family)
     *
     * @return булевое значение существует ли имя семейства столбцов (Column Family) в таблице
     */
    public boolean checkFamilyInTable(String tableName,
                                             String columnFamilyName) {

        List<ColumnFamilyDescriptor> arr = getTableColumnFamilies(connection, tableName);
        return arr.stream().anyMatch(colDesc -> colDesc.getNameAsString().equals(columnFamilyName));
    }

    /***
     *
     * @param tableName имя таблицы в БД
     * @param saveColumnFamilies сохранить семейства столбцов (Column Family)?
     */
    public void purgeTable(String tableName,
                                  boolean saveColumnFamilies) {

        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();

        if (saveColumnFamilies) {
            columnFamilyDescriptors = getTableColumnFamilies(connection, tableName);
        }
        deleteTable(tableName);
        createTable(tableName, columnFamilyDescriptors);
    }

    /***
     *

     * @param tableName имя таблицы в БД
     */

    @SneakyThrows
    public void deleteTable(String tableName) {
        connection.getAdmin().deleteTable(TableName.valueOf(tableName));
    }
}
