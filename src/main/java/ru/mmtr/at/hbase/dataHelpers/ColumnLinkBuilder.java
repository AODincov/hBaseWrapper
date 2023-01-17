package ru.mmtr.at.hbase.dataHelpers;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Assertions;


/***
 * tableName имя таблицы в БД
 * rowKeyName первичный ключ таблицы
 * columnFamilyName имя семейства столбцов (Column Family)
 * columnName имя семейства столбцов (Column Family)
 */

@Setter
@Accessors(chain = true)
public class ColumnLinkBuilder {
    String tableName;
    String rowKeyName;
    String columnFamilyName;
    String columnName;

    public ColumnLink build() {

        assert tableName != null;
        assert rowKeyName != null;
        assert columnFamilyName != null;
        assert columnName != null;

        return new ColumnLink(tableName, rowKeyName, columnFamilyName, columnName);
    }
}
