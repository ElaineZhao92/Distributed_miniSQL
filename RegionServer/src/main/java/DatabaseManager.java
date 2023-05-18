
import miniSQL.INDEXMANAGER.Index;
import miniSQL.CATALOGMANAGER.CatalogManager;
import miniSQL.CATALOGMANAGER.Table;

import java.util.LinkedHashMap;
import java.util.Map;
import java.io.IOException;

public class DatabaseManager {
    private LinkedHashMap<String, Table> tables;
    private LinkedHashMap<String, Index> indices;

    public DatabaseManager() throws IOException {
        CatalogManager.initialCatalog();
        this.tables = CatalogManager.getTables();
        this.indices = CatalogManager.getIndex();
    }

    public String getMetaInfo() {  // 获取当前从节点中所有数据表和索引的信息，用于后续发送给主节点，并进行查询
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Table> stringTableEntry : tables.entrySet()) {
            result.append(((Map.Entry) stringTableEntry).getKey()).append(" ");
        }
        return result.toString();
    }


}