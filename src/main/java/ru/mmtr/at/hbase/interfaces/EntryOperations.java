package ru.mmtr.at.hbase.interfaces;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.mmtr.at.hbase.connection.ConnectionFactory;

import java.util.*;

//todo method's params container class
//todo DI connection
public class EntryOperations {

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */

    @SneakyThrows
    public static Map<Long, byte[]> getAllVersionsFromTable(
            Connection c,
            String tableName,
            String rowKeyName,
            String columnFamilyName,
            String columnName
    ) {
        return new HashMap<>(getQualifiersRawData(c, tableName, rowKeyName, columnFamilyName).get(Bytes.toBytes(columnName)));
    }

    /***
     *  *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     *
     * @return содержимое пересечения RowKey и ColumnFamily (все Columns\Qualifier)
     */

    @SneakyThrows
    private static NavigableMap<byte[], NavigableMap<Long, byte[]>> getQualifiersRawData(
            Connection c,
            String tableName,
            String rowKeyName,
            String columnFamilyName
    ) {

        Get getQuery = new Get(Bytes.toBytes(rowKeyName));
        getQuery.readAllVersions();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions =
                c.getTable(TableName.valueOf(tableName)).get(getQuery).getMap();

        return allVersions.get(Bytes.toBytes(columnFamilyName));
    }

    /***
     *
     * @param qualifiers содержимое пересечения RowKey и ColumnFamily (все Columns\Qualifier)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */
    @SneakyThrows
    private static NavigableMap<Long, byte[]> getSingleQualifierRawData(
            NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers,
            String columnName
    ) {
        return qualifiers.get(Bytes.toBytes(columnName));
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */
    @SneakyThrows
    public static NavigableMap<Long, byte[]> getSingleQualifierRawData(
            Connection c,
            String tableName,
            String rowKeyName,
            String columnFamilyName,
            String columnName
    ) {
        return getSingleQualifierRawData(getQualifiersRawData(c, tableName, rowKeyName, columnFamilyName), columnName);
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @param data данные для добавления в виде List<byte[]>.
     */
    @SneakyThrows
    public static void addDataToTable(Connection c,
                                      String tableName,
                                      String rowKeyName,
                                      String columnFamilyName,
                                      String columnName,
                                      List<byte[]> data) {
        Table table = c.getTable(TableName.valueOf(tableName));
        Put p = new Put(Bytes.toBytes(rowKeyName));
        for (byte[] val : data) {
            p.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), val);
            table.put(p);
        }
        table.put(p);
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @param data данные для добавления
     *
     *
     */
    @SneakyThrows
    public static void addDataToTable(Connection c,
                                      String tableName,
                                      String rowKeyName,
                                      String columnFamilyName,
                                      String columnName,
                                      Map<Long, byte[]> data) {
        Table table = c.getTable(TableName.valueOf(tableName));
        Put p = new Put(Bytes.toBytes(rowKeyName));
        data.forEach((timestamp, value) ->
                p.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(columnName), timestamp, value)
        );
        table.put(p);
    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @param timestamp временная метка записи
     *
     * Удаляет запись с указанной временной меткой
     */
    @SneakyThrows
    public static void deleteColumn(Connection c,
                                    String tableName,
                                    String rowKeyName,
                                    String columnFamilyName,
                                    String columnName,
                                    Long timestamp){

        Table table = c.getTable(TableName.valueOf(tableName));
        Delete deleteOperation = new Delete(Bytes.toBytes(rowKeyName));

        deleteOperation.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(columnName),timestamp);
        table.delete(deleteOperation);

    }

    /***
     *
     * @param c коннект к бд. смотри {@link ConnectionFactory#getConnection()}
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     *
     * Удаляет самую свежую запись
     *
     */

    @SneakyThrows
    public static void deleteColumn(Connection c,
                                    String tableName,
                                    String rowKeyName,
                                    String columnFamilyName,
                                    String columnName){

        Table table = c.getTable(TableName.valueOf(tableName));
        Delete deleteOperation = new Delete(Bytes.toBytes(rowKeyName));

        deleteOperation.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(columnName));
        table.delete(deleteOperation);
    }

}
