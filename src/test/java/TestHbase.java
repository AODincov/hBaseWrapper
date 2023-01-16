import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.mmtr.at.hbase.connection.ConnectionFactory;
import ru.mmtr.at.hbase.interfaces.TableOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class TestHbase {

    //todo parametrized tests?
    Connection c = ConnectionFactory.getConnection();
    TableOperations tableOperations = new TableOperations(c);


    @Test
    public void addTableTest() {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyThreeVersions")).setMaxVersions(3).build());
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyTwoVersions")).setMaxVersions(2).build());

        tableOperations.createTable(
                RandomStringUtils.random(4, true, false),
                columnFamilyDescriptors);

        //todo checks
    }

    @Test
    public void deleteTableTest() {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyThreeVersions")).setMaxVersions(3).build());
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyTwoVersions")).setMaxVersions(2).build());

        String tableName = RandomStringUtils.random(4, true, false);

        tableOperations.createTable(
                tableName,
                columnFamilyDescriptors);

        //todo checks
    }

}
