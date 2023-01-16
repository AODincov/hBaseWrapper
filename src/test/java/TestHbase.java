import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import ru.mmtr.at.hbase.connection.ConnectionFactory;
import ru.mmtr.at.hbase.interfaces.TableOperations;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

public class TestHbase {

    @SneakyThrows
    @Test
    public void debugStuff() {

        Connection c = ConnectionFactory.getConnection();

        TableName table1 = TableName.valueOf("tab2");
        Table table = c.getTable(table1);

        String family1 = "Family1";
        String fam1 = "fam1";
        String family2 = "Family2";

        Get g = new Get(Bytes.toBytes("row1")); //rowKey
        g.readAllVersions();
        Result r = table.get(g);

        List<byte[]> res = new ArrayList<>();

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> allVersions = r.getMap();

        NavigableMap<Long, byte[]> resMap = allVersions.firstEntry().getValue().firstEntry().getValue();

        resMap.forEach(

                (k, v) -> res.add(v)
        );

        System.out.println();

    }

    @Test
    public void addTableTest() {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyThreeVersions")).setMaxVersions(3).build());
        columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("familyTwoVersions")).setMaxVersions(2).build());


        TableOperations.createTable(
                ConnectionFactory.getConnection(),
                RandomStringUtils.random(4, true, false),
                columnFamilyDescriptors);

        //todo checks

    }
}
