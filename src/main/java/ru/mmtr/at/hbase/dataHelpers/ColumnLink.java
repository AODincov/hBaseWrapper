package ru.mmtr.at.hbase.dataHelpers;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/***
 * {@link ColumnLinkBuilder}
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
public class ColumnLink {

    String tableName;
    String rowKeyName;
    String columnFamilyName;
    String columnName;

}
