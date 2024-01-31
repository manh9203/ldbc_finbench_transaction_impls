package org.ldbcouncil.finbench.impls.kuzu;
import com.kuzudb.*;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.impls.common.BaseDbConnectionState;

public class KuzuDbConnectionState extends BaseDbConnectionState<KuzuQueryStore> {
    static Logger logger = LogManager.getLogger("KuzuDbConnectionState");
    private final KuzuDatabase db;
    protected KuzuConnection conn;

    public KuzuDbConnectionState(Map<String, String> properties, KuzuQueryStore store) {
        super(properties, store);
        String dbPath = properties.get("dbPath");
        db = new KuzuDatabase(dbPath);
        conn = new KuzuConnection(db);
    }

    public KuzuConnection getConnection() {
        return conn;
    }

    @Override
    public void close() {
        try {
            conn.destroy();
            db.destroy();
        } catch (Exception e) {
            logger.error("KuzuDB closed", e);
        }
    }
}
