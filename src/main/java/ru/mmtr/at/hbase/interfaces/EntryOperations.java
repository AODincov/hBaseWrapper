package ru.mmtr.at.hbase.interfaces;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.mmtr.at.hbase.dataHelpers.ColumnLink;
import ru.mmtr.at.hbase.dataHelpers.ColumnLinkBuilder;

import java.util.*;

//todo method's params container class
@AllArgsConstructor
public class EntryOperations {

    Connection connection; //коннект к бд. смотри ConnectionFactory#getConnection()

    /***
     *
     *  tableName имя таблицы в БД
     *  rowKeyName первичный ключ таблицы
     *  columnFamilyName имя семейства столбцов (Column Family)
     *  columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */

    @SneakyThrows
    public Map<Long, byte[]> getAllVersionsFromTable(
            ColumnLink link
    ) {
        return new HashMap<>(
                getQualifiersRawData(
                        link.getTableName(),
                        link.getRowKeyName(),
                        link.getColumnFamilyName()
                ).get(Bytes.toBytes(link.getColumnName()))
        );
    }

    /***
     *
     * @param tableName имя таблицы в БД
     * @param rowKeyName первичный ключ таблицы
     * @param columnFamilyName имя семейства столбцов (Column Family)
     *
     * @return содержимое пересечения RowKey и ColumnFamily (все Columns\Qualifier)
     */

    @SneakyThrows
    private NavigableMap<byte[], NavigableMap<Long, byte[]>> getQualifiersRawData(
            String tableName,
            String rowKeyName,
            String columnFamilyName
    ) {

        Get getQuery = new Get(Bytes.toBytes(rowKeyName));
        getQuery.readAllVersions();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions =
                connection.getTable(TableName.valueOf(tableName)).get(getQuery).getMap();

        return allVersions.get(Bytes.toBytes(columnFamilyName));
    }

    /***
     *
     * @param qualifiers содержимое пересечения RowKey и ColumnFamily (все Columns\Qualifier)
     * @param columnName имя столбца (параметра) ВНУТРИ семества столбцов (Column\Qualifier)
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */
    @SneakyThrows
    private NavigableMap<Long, byte[]> getSingleQualifierRawData(
            NavigableMap<byte[], NavigableMap<Long, byte[]>> qualifiers,
            String columnName
    ) {
        return qualifiers.get(Bytes.toBytes(columnName));
    }

    /***
     * @param columnLink данные для поиска столбца {@link ColumnLinkBuilder}
     * @return все значения Qualifier, отсортированные по timestamp от новых к старым. K - Timestamp (UNIX Time), V - данные в виде Byte[]
     */
    @SneakyThrows
    public NavigableMap<Long, byte[]> getSingleQualifierRawData(
            ColumnLink columnLink
    ) {
        return getSingleQualifierRawData(
                getQualifiersRawData(
                        columnLink.getTableName(),
                        columnLink.getRowKeyName(),
                        columnLink.getColumnFamilyName()),
                columnLink.getColumnName());
    }

    /***
     * @param columnLink данные для поиска столбца {@link ColumnLinkBuilder}
     * @param data данные для добавления в виде List<byte[]>.
     */
    @SneakyThrows
    public void addDataToTable(ColumnLink columnLink, List<byte[]> data) {
        Table table = connection.getTable(TableName.valueOf(columnLink.getTableName()));
        Put p = new Put(Bytes.toBytes(columnLink.getRowKeyName()));
        for (byte[] val : data) {
            p.addColumn(Bytes.toBytes(columnLink.getColumnFamilyName()), Bytes.toBytes(columnLink.getColumnName()), val);
            table.put(p);
        }
        table.put(p);
    }

    /***
     *
     * @param columnLink данные для поиска столбца {@link ColumnLinkBuilder}
     * @param data данные для добавления
     *
     *
     */
    @SneakyThrows
    public void addDataToTable(ColumnLink columnLink,
                               Map<Long, byte[]> data) {
        Table table = connection.getTable(TableName.valueOf(columnLink.getTableName()));
        Put p = new Put(Bytes.toBytes(columnLink.getRowKeyName()));
        data.forEach((timestamp, value) ->
                p.addColumn(
                        Bytes.toBytes(columnLink.getColumnFamilyName()),
                        Bytes.toBytes(columnLink.getColumnName()),
                        timestamp,
                        value)
        );
        table.put(p);
    }

    /***
     *
     * @param timestamp временная метка записи
     * @param columnLink данные для поиска столбца {@link ColumnLinkBuilder}

     * Удаляет запись с указанной временной меткой
     */
    @SneakyThrows
    public void deleteColumn(ColumnLink columnLink,
                             Long timestamp) {

        Table table = connection.getTable(TableName.valueOf(columnLink.getTableName()));
        Delete deleteOperation = new Delete(Bytes.toBytes(columnLink.getRowKeyName()));

        deleteOperation.addColumn(
                Bytes.toBytes(columnLink.getColumnFamilyName()),
                Bytes.toBytes(columnLink.getColumnName()),
                timestamp);
        table.delete(deleteOperation);

    }

    /***
     *
     * @param columnLink данные для поиска столбца {@link ColumnLinkBuilder}
     * Удаляет самую свежую запись
     *
     */

    @SneakyThrows
    public void deleteColumn(ColumnLink columnLink) {

        Table table = connection.getTable(TableName.valueOf(columnLink.getTableName()));
        Delete deleteOperation = new Delete(Bytes.toBytes(columnLink.getRowKeyName()));

        deleteOperation.addColumn(Bytes.toBytes(columnLink.getColumnFamilyName()), Bytes.toBytes(columnLink.getColumnName()));
        table.delete(deleteOperation);
    }

}
