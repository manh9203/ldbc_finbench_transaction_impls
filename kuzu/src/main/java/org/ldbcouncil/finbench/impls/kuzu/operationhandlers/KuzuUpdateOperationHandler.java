package org.ldbcouncil.finbench.impls.kuzu.operationhandlers;
import com.kuzudb.*;

import java.util.Map;
import org.ldbcouncil.finbench.impls.kuzu.KuzuDbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;

public abstract class KuzuUpdateOperationHandler<
    TOperation extends Operation<LdbcNoResult>>
    implements UpdateOperationHandler<TOperation, KuzuDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 KuzuDbConnectionState state,
                                 ResultReporter resultReporter)
        throws DbException {
        KuzuConnection conn = state.getConnection();

        String queryString = getQuery(state, operation);
        Map<String, KuzuValue> params = getParams(state, operation);
        if (queryString.contains("$truncationOrder")) {
            try {
                queryString = queryString.replace("$truncationOrder", (String) params.get("truncationOrder").getValue());
                queryString = queryString.replace("$truncationLimit", String.valueOf((int) params.get("truncationLimit").getValue()));
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        try {
            KuzuPreparedStatement query = conn.prepare(queryString);
            KuzuQueryResult statementResult = conn.execute(query, params);
            while (statementResult.hasNext()) {
                statementResult.getNext();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

    protected abstract String getQuery(KuzuDbConnectionState state, TOperation operation);

    protected abstract Map<String, KuzuValue> getParams(KuzuDbConnectionState state, TOperation operation);

}
