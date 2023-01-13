package ru.mmtr.at.hbase.interfaces;

import lombok.SneakyThrows;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import ru.mmtr.at.hbase.connection.ConnectionFactory;

import java.util.*;

public class TableOperations {

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
    private static NavigableMap<Long, byte[]> getSingleQualifierRawData(
            Connection c,
            String tableName,
            String rowKeyName,
            String columnFamilyName,
            String columnName
    ) {
        return getSingleQualifierRawData(getQualifiersRawData(c, tableName, rowKeyName, columnFamilyName), columnName);
    }


    @SneakyThrows
    public static void createTable(Connection c,
                                   String tableName,
                                   List<ColumnFamilyDescriptor> columnFamilies) {

        TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        tableBuilder.setColumnFamilies(columnFamilies);
        c.getAdmin().createTable(tableBuilder.build());
    }
}
