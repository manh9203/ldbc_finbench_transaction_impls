package org.ldbcouncil.finbench.impls.kuzu;
import com.kuzudb.*;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.truncation.TruncationOrder;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write13;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write14;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write15;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write16;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write17;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write18;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write19;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write9;
import org.ldbcouncil.finbench.impls.common.QueryStore;
import org.ldbcouncil.finbench.impls.common.QueryType;

public class KuzuQueryStore extends QueryStore {

    public KuzuQueryStore(String path) throws DbException {
        super(path, ".cypher");
    }

    protected String getQuery(QueryType queryType) {
        return queries.get(queryType);
    }

    public Map<String, KuzuValue> getParamsComplexRead1(ComplexRead1 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead1.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead1.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead1.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead1.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead1.TRUNCATION_ORDER, new KuzuValue(new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC").equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead2(ComplexRead2 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead2.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead2.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead2.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead2.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead2.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead3(ComplexRead3 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead3.ID1, new KuzuValue(operation.getId1()))
                .put(ComplexRead3.ID2, new KuzuValue(operation.getId2()))
                .put(ComplexRead3.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead3.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead4(ComplexRead4 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead4.ID1, new KuzuValue(operation.getId1()))
                .put(ComplexRead4.ID2, new KuzuValue(operation.getId2()))
                .put(ComplexRead4.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead4.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead5(ComplexRead5 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead5.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead5.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead5.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead5.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead5.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead6(ComplexRead6 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead6.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead6.THRESHOLD1, new KuzuValue(operation.getThreshold1()))
                .put(ComplexRead6.THRESHOLD2, new KuzuValue(operation.getThreshold2()))
                .put(ComplexRead6.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead6.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead6.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead6.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead7(ComplexRead7 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead7.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead7.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(ComplexRead7.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead7.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead7.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead7.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead8(ComplexRead8 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead8.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead8.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(ComplexRead8.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead8.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead8.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead8.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead9(ComplexRead9 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead9.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead9.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(ComplexRead9.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead9.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead9.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead9.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead10(ComplexRead10 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead10.PID1, new KuzuValue(operation.getPid1()))
                .put(ComplexRead10.PID2, new KuzuValue(operation.getPid2()))
                .put(ComplexRead10.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead10.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead11(ComplexRead11 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead11.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead11.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead11.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead11.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead11.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsComplexRead12(ComplexRead12 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ComplexRead12.ID, new KuzuValue(operation.getId()))
                .put(ComplexRead12.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ComplexRead12.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ComplexRead12.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ComplexRead12.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead1(SimpleRead1 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead1.ID, new KuzuValue(operation.getId()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead2(SimpleRead2 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead2.ID, new KuzuValue(operation.getId()))
                .put(SimpleRead2.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(SimpleRead2.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead3(SimpleRead3 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead3.ID, new KuzuValue(operation.getId()))
                .put(SimpleRead3.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(SimpleRead3.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(SimpleRead3.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead4(SimpleRead4 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead4.ID, new KuzuValue(operation.getId()))
                .put(SimpleRead4.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(SimpleRead4.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(SimpleRead4.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead5(SimpleRead5 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead5.ID, new KuzuValue(operation.getId()))
                .put(SimpleRead5.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(SimpleRead5.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(SimpleRead5.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamsSimpleRead6(SimpleRead6 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(SimpleRead6.ID, new KuzuValue(operation.getId()))
                .put(SimpleRead6.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(SimpleRead6.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite1(Write1 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write1.PERSON_ID, new KuzuValue(operation.getPersonId()))
                .put(Write1.PERSON_NAME, new KuzuValue(operation.getPersonName()))
                .put(Write1.IS_BLOCKED, new KuzuValue(operation.getIsBlocked()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite2(Write2 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write2.COMPANY_ID, new KuzuValue(operation.getCompanyId()))
                .put(Write2.COMPANY_NAME, new KuzuValue(operation.getCompanyName()))
                .put(Write2.IS_BLOCKED, new KuzuValue(operation.getIsBlocked()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite3(Write3 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write3.MEDIUM_ID, new KuzuValue(operation.getMediumId()))
                .put(Write3.MEDIUM_TYPE, new KuzuValue(operation.getMediumType()))
                .put(Write3.IS_BLOCKED, new KuzuValue(operation.getIsBlocked()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite4(Write4 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write4.PERSON_ID, new KuzuValue(operation.getPersonId()))
                .put(Write4.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .put(Write4.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write4.ACCOUNT_BLOCKED, new KuzuValue(operation.getAccountBlocked()))
                .put(Write4.ACCOUNT_TYPE, new KuzuValue(operation.getAccountType()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite5(Write5 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write5.COMPANY_ID, new KuzuValue(operation.getCompanyId()))
                .put(Write5.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .put(Write5.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write5.ACCOUNT_BLOCKED, new KuzuValue(operation.getAccountBlocked()))
                .put(Write5.ACCOUNT_TYPE, new KuzuValue(operation.getAccountType()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite6(Write6 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write6.PERSON_ID, new KuzuValue(operation.getPersonId()))
                .put(Write6.LOAN_ID, new KuzuValue(operation.getLoanId()))
                .put(Write6.LOAN_AMOUNT, new KuzuValue(operation.getLoanAmount()))
                .put(Write6.BALANCE, new KuzuValue(operation.getBalance()))
                .put(Write6.TIME, new KuzuValue(operation.getTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite7(Write7 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write7.COMPANY_ID, new KuzuValue(operation.getCompanyId()))
                .put(Write7.LOAN_ID, new KuzuValue(operation.getLoanId()))
                .put(Write7.LOAN_AMOUNT, new KuzuValue(operation.getLoanAmount()))
                .put(Write7.BALANCE, new KuzuValue(operation.getBalance()))
                .put(Write7.TIME, new KuzuValue(operation.getTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite8(Write8 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write8.PERSON_ID, new KuzuValue(operation.getPersonId()))
                .put(Write8.COMPANY_ID, new KuzuValue(operation.getCompanyId()))
                .put(Write8.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write8.RATIO, new KuzuValue((float) operation.getRatio()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite9(Write9 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write9.COMPANY_ID1, new KuzuValue(operation.getCompanyId1()))
                .put(Write9.COMPANY_ID2, new KuzuValue(operation.getCompanyId2()))
                .put(Write9.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write9.RATIO, new KuzuValue((float) operation.getRatio()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite10(Write10 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write10.PERSON_ID1, new KuzuValue(operation.getPersonId1()))
                .put(Write10.PERSON_ID2, new KuzuValue(operation.getPersonId2()))
                .put(Write10.TIME, new KuzuValue(operation.getTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite11(Write11 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write11.COMPANY_ID1, new KuzuValue(operation.getCompanyId1()))
                .put(Write11.COMPANY_ID2, new KuzuValue(operation.getCompanyId2()))
                .put(Write11.TIME, new KuzuValue(operation.getTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite12(Write12 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write12.ACCOUNT_ID1, new KuzuValue(operation.getAccountId1()))
                .put(Write12.ACCOUNT_ID2, new KuzuValue(operation.getAccountId2()))
                .put(Write12.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write12.AMOUNT, new KuzuValue(operation.getAmount()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite13(Write13 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write13.ACCOUNT_ID1, new KuzuValue(operation.getAccountId1()))
                .put(Write13.ACCOUNT_ID2, new KuzuValue(operation.getAccountId2()))
                .put(Write13.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write13.AMOUNT, new KuzuValue(operation.getAmount()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite14(Write14 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write14.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .put(Write14.LOAN_ID, new KuzuValue(operation.getLoanId()))
                .put(Write14.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write14.AMOUNT, new KuzuValue(operation.getAmount()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite15(Write15 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write15.LOAN_ID, new KuzuValue(operation.getLoanId()))
                .put(Write15.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .put(Write15.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(Write15.AMOUNT, new KuzuValue(operation.getAmount()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite16(Write16 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write16.MEDIUM_ID, new KuzuValue(operation.getMediumId()))
                .put(Write16.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .put(Write16.TIME, new KuzuValue(operation.getTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite17(Write17 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write17.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite18(Write18 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write18.ACCOUNT_ID, new KuzuValue(operation.getAccountId()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamWrite19(Write19 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(Write19.PERSON_ID, new KuzuValue(operation.getPersonId()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamReadWrite1(ReadWrite1 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ReadWrite1.SRC_ID, new KuzuValue(operation.getSrcId()))
                .put(ReadWrite1.DST_ID, new KuzuValue(operation.getDstId()))
                .put(ReadWrite1.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(ReadWrite1.AMOUNT, new KuzuValue(operation.getAmount()))
                .put(ReadWrite1.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ReadWrite1.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamReadWrite2(ReadWrite2 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ReadWrite2.SRC_ID, new KuzuValue(operation.getSrcId()))
                .put(ReadWrite2.DST_ID, new KuzuValue(operation.getDstId()))
                .put(ReadWrite2.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(ReadWrite2.AMOUNT_THRESHOLD, new KuzuValue(operation.getAmountThreshold()))
                .put(ReadWrite2.AMOUNT, new KuzuValue(operation.getAmount()))
                .put(ReadWrite2.START_TIME, new KuzuValue(operation.getStartTime().toInstant()))
                .put(ReadWrite2.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ReadWrite2.RATIO_THRESHOLD, new KuzuValue((float) operation.getRatioThreshold()))
                .put(ReadWrite2.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ReadWrite2.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public Map<String, KuzuValue> getParamReadWrite3(ReadWrite3 operation) {
        try {
            return new ImmutableMap.Builder<String, KuzuValue>()
                .put(ReadWrite3.SRC_ID, new KuzuValue(operation.getSrcId()))
                .put(ReadWrite3.DST_ID, new KuzuValue(operation.getDstId()))
                .put(ReadWrite2.TIME, new KuzuValue(operation.getTime().toInstant()))
                .put(ReadWrite3.THRESHOLD, new KuzuValue(operation.getThreshold()))
                .put(ReadWrite3.START_TIME, new KuzuValue(operation.getStartTime().getTime()))
                .put(ReadWrite3.END_TIME, new KuzuValue(operation.getEndTime().toInstant()))
                .put(ReadWrite3.TRUNCATION_LIMIT, new KuzuValue(operation.getTruncationLimit()))
                .put(ReadWrite3.TRUNCATION_ORDER, new KuzuValue(operation.getTruncationOrder().name().equals(TruncationOrder.TIMESTAMP_DESCENDING.name()) ? "DESC" : "ASC"))
                .build();
        } catch (KuzuObjectRefDestroyedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
}
