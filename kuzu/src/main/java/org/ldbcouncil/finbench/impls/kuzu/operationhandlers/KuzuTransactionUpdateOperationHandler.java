package org.ldbcouncil.finbench.impls.kuzu.operationhandlers;
import com.kuzudb.*;

import java.util.Map;
import java.util.HashMap;
import org.ldbcouncil.finbench.impls.kuzu.KuzuDbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.Operation;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.impls.common.operationhandlers.UpdateOperationHandler;

public abstract class KuzuTransactionUpdateOperationHandler<
    TOperation extends Operation<LdbcNoResult>>
    implements UpdateOperationHandler<TOperation, KuzuDbConnectionState> {

    @Override
    public void executeOperation(TOperation operation,
                                 KuzuDbConnectionState state,
                                 ResultReporter resultReporter) throws DbException {
        KuzuConnection conn = state.getConnection();

        String queryString = getQuery(state, operation);
        Map<String, KuzuValue> params = getParams(state, operation);

        // TODO: implement truncation edges, commented this part for now
        // if (queryString.contains("$truncationOrder")) {
        //     try {
        //         queryString = queryString.replace("$truncationOrder", (String) params.get("truncationOrder").getValue());
        //         queryString = queryString.replace("$truncationLimit", String.valueOf((int) params.get("truncationLimit").getValue()));
        //     } catch (KuzuObjectRefDestroyedException e) {
        //         // TODO Auto-generated catch block
        //         e.printStackTrace();
        //     }
        // }

        // disable params for truncation order and limit
        // will remove this part when done with implementing truncation
        if (params.containsKey("truncationOrder")) {
            Map<String, KuzuValue> newParams = new HashMap<>(params);
            newParams.remove("truncationOrder");
            newParams.remove("truncationLimit");
            params = newParams;
        }

        String[] txns = queryString.split("BEGIN|COMMIT", 1000);

        try {
            for (String txn : txns) {
                if (txn.trim().isEmpty()) {
                    continue;
                }
                conn.beginWriteTransaction();
                String[] queries = txn.split("QUERY", 1000);
                int successNum = 0;
                boolean isSuccess = true;
                try {
                    for (String query : queries) {
                        if (query.trim().isEmpty()) {
                            continue;
                        }
                        KuzuPreparedStatement preparedQuery = conn.prepare(query);
                        KuzuQueryResult statementResult = conn.execute(preparedQuery, params);
                        if (statementResult.hasNext()) {
                            KuzuFlatTuple tuple = statementResult.getNext();
                            if (!(Boolean) tuple.getValue(0).getValue()) {
                                isSuccess = false;
                                break;
                            }
                        } else {
                            isSuccess = false;
                            break;
                        }
                        successNum++;
                    }
                } finally {
                    if (!isSuccess) {
                        conn.rollback();
                    }
                    conn.commit();
                }
                // Whether to do step 4
                // step 1 is success
                // step 2 is success
                // step 3 is failure
                if (successNum != 2) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
    }

    protected abstract String getQuery(KuzuDbConnectionState state, TOperation operation);

    protected abstract Map<String, KuzuValue> getParams(KuzuDbConnectionState state, TOperation operation);
}
