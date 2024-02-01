package org.ldbcouncil.finbench.impls.kuzu;
import com.kuzudb.*;

import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write14;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write15;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write16;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write17;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write18;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write19;
import org.ldbcouncil.finbench.impls.common.QueryType;
import org.ldbcouncil.finbench.impls.kuzu.operationhandlers.KuzuListOperationHandler;
import org.ldbcouncil.finbench.impls.kuzu.operationhandlers.KuzuTransactionUpdateOperationHandler;
import org.ldbcouncil.finbench.impls.kuzu.operationhandlers.KuzuUpdateOperationHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.result.Path;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead10Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead11Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead12Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead1Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead2Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead3Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead6Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead7Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead8Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ComplexRead9Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.ReadWrite3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead1Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead2Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead3Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead4Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead5Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.SimpleRead6Result;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write1;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write10;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write11;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write12;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write13;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write2;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write3;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write4;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write5;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write6;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write7;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write8;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.Write9;

public class KuzuDb extends Db {
    static Logger logger = LogManager.getLogger("KuzuDb");
    KuzuDbConnectionState dcs;

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        logger.info("KuzuDB initialized");
        dcs = new KuzuDbConnectionState(properties, new KuzuQueryStore(properties.get("queryDir")));

        // complex reads
        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        registerOperationHandler(ComplexRead2.class, ComplexRead2Handler.class);
        registerOperationHandler(ComplexRead3.class, ComplexRead3Handler.class);
        registerOperationHandler(ComplexRead4.class, ComplexRead4Handler.class);
        registerOperationHandler(ComplexRead5.class, ComplexRead5Handler.class);
        registerOperationHandler(ComplexRead6.class, ComplexRead6Handler.class);
        registerOperationHandler(ComplexRead7.class, ComplexRead7Handler.class);
        registerOperationHandler(ComplexRead8.class, ComplexRead8Handler.class);
        registerOperationHandler(ComplexRead9.class, ComplexRead9Handler.class);
        registerOperationHandler(ComplexRead10.class, ComplexRead10Handler.class);
        registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
        registerOperationHandler(ComplexRead12.class, ComplexRead12Handler.class);

        // simple reads
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);

        // writes
        registerOperationHandler(Write1.class, Write1Handler.class);
        registerOperationHandler(Write2.class, Write2Handler.class);
        registerOperationHandler(Write3.class, Write3Handler.class);
        registerOperationHandler(Write4.class, Write4Handler.class);
        registerOperationHandler(Write5.class, Write5Handler.class);
        registerOperationHandler(Write6.class, Write6Handler.class);
        registerOperationHandler(Write7.class, Write7Handler.class);
        registerOperationHandler(Write8.class, Write8Handler.class);
        registerOperationHandler(Write9.class, Write9Handler.class);
        registerOperationHandler(Write10.class, Write10Handler.class);
        registerOperationHandler(Write11.class, Write11Handler.class);
        registerOperationHandler(Write12.class, Write12Handler.class);
        registerOperationHandler(Write13.class, Write13Handler.class);
        registerOperationHandler(Write14.class, Write14Handler.class);
        registerOperationHandler(Write15.class, Write15Handler.class);
        registerOperationHandler(Write16.class, Write16Handler.class);
        registerOperationHandler(Write17.class, Write17Handler.class);
        registerOperationHandler(Write18.class, Write18Handler.class);
        registerOperationHandler(Write19.class, Write19Handler.class);

        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("KuzuDB closed");
        dcs.close();
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return dcs;
    }

    public static class ComplexRead1Handler extends
        KuzuListOperationHandler<ComplexRead1Result, ComplexRead1> {

        @Override
        protected ComplexRead1Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead1Result result = new ComplexRead1Result(0, 0, 0, null);
            try {
                result = new ComplexRead1Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (int) tuple.getValue(1).getValue(),
                    Long.parseLong((String) tuple.getValue(2).getValue()),
                    (String) tuple.getValue(3).getValue());
                return result;
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getComplexRead1(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead1);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead1 operation) {
            return state.getQueryStore().getParamsComplexRead1(operation);
        }
    }

    public static class ComplexRead2Handler extends
        KuzuListOperationHandler<ComplexRead2Result, ComplexRead2> {

        @Override
        protected ComplexRead2Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead2Result result = new ComplexRead2Result(0, 0, 0);
            try {
                result = new ComplexRead2Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (double) tuple.getValue(1).getValue(),
                    (double) tuple.getValue(2).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getComplexRead2(operation);
        }        
        
        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead2);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead2 operation) {
            return state.getQueryStore().getParamsComplexRead2(operation);
        }
    }

    public static class ComplexRead3Handler extends
        KuzuListOperationHandler<ComplexRead3Result, ComplexRead3> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getComplexRead3(operation);
        }


        @Override
        protected ComplexRead3Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead3Result result = new ComplexRead3Result(0);
            try {
                result = new ComplexRead3Result((long) tuple.getValue(0).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead3);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead3 operation) {
            return state.getQueryStore().getParamsComplexRead3(operation);
        }
    }

    public static class ComplexRead4Handler extends
        KuzuListOperationHandler<ComplexRead4Result, ComplexRead4> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getComplexRead4(operation);
        }


        @Override
        protected ComplexRead4Result convertSingleResult(KuzuFlatTuple tuple) {
            
            ComplexRead4Result result = new ComplexRead4Result(0, 0, 0, 0, 0, 0, 0);
            try {
                result = new ComplexRead4Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (long) tuple.getValue(1).getValue(),
                    (double) tuple.getValue(2).getValue(),
                    (double) tuple.getValue(3).getValue(),
                    (long) tuple.getValue(4).getValue(),
                    (double) tuple.getValue(5).getValue(),
                    (double) tuple.getValue(6).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead4);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead4 operation) {
            return state.getQueryStore().getParamsComplexRead4(operation);
        }
    }

    public static class ComplexRead5Handler extends
        KuzuListOperationHandler<ComplexRead5Result, ComplexRead5> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getComplexRead5(operation);
        }


        @Override
        protected ComplexRead5Result convertSingleResult(KuzuFlatTuple tuple) {
            Path path = new Path();
            List<Object> ids = new ArrayList<>();
            try {
                ids = tuple.getValue(0).getValue();
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            for (Object id : ids) {
                path.addId(Long.parseLong((String) id));
            }
            ComplexRead5Result result = new ComplexRead5Result(path);
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead5);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead5 operation) {
            return state.getQueryStore().getParamsComplexRead5(operation);
        }
    }

    public static class ComplexRead6Handler extends
        KuzuListOperationHandler<ComplexRead6Result, ComplexRead6> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getComplexRead6(operation);
        }


        @Override
        protected ComplexRead6Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead6Result result = new ComplexRead6Result(0, 0, 0);
            try {
                result = new ComplexRead6Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (double) tuple.getValue(1).getValue(),
                    (double) tuple.getValue(2).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead6);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead6 operation) {
            return state.getQueryStore().getParamsComplexRead6(operation);
        }
    }

    public static class ComplexRead7Handler extends
        KuzuListOperationHandler<ComplexRead7Result, ComplexRead7> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getComplexRead7(operation);
        }

        @Override
        protected ComplexRead7Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead7Result result = new ComplexRead7Result(0, 0, 0);
            try {
                result = new ComplexRead7Result(
                    (int) tuple.getValue(0).getValue(),
                    (int) tuple.getValue(1).getValue(),
                    (float) tuple.getValue(2).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }
        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead7);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead7 operation) {
            return state.getQueryStore().getParamsComplexRead7(operation);
        }
    }

    public static class ComplexRead8Handler extends
        KuzuListOperationHandler<ComplexRead8Result, ComplexRead8> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getComplexRead8(operation);
        }


        @Override
        protected ComplexRead8Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead8Result result = new ComplexRead8Result(0, 0, 0);
            try {
                result = new ComplexRead8Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (float) tuple.getValue(1).getValue(),
                    (int) tuple.getValue(2).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead8);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead8 operation) {
            return state.getQueryStore().getParamsComplexRead8(operation);
        }
    }

    public static class ComplexRead9Handler extends
        KuzuListOperationHandler<ComplexRead9Result, ComplexRead9> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getComplexRead9(operation);
        }


        @Override
        protected ComplexRead9Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead9Result result = new ComplexRead9Result(0, 0, 0);
            try {
                result = new ComplexRead9Result(
                    (float) tuple.getValue(0).getValue(),
                    (float) tuple.getValue(1).getValue(),
                    (float) tuple.getValue(2).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }
        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead9);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead9 operation) {
            return state.getQueryStore().getParamsComplexRead9(operation);
        }
    }

    public static class ComplexRead10Handler extends
        KuzuListOperationHandler<ComplexRead10Result, ComplexRead10> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getComplexRead10(operation);
        }


        @Override
        protected ComplexRead10Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead10Result result = new ComplexRead10Result(0);
            try {
                result = new ComplexRead10Result(
                    (float) tuple.getValue(0).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead10);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead10 operation) {
            return state.getQueryStore().getParamsComplexRead10(operation);
        }
    }

    public static class ComplexRead11Handler extends
        KuzuListOperationHandler<ComplexRead11Result, ComplexRead11> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getComplexRead11(operation);
        }


        @Override
        protected ComplexRead11Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead11Result result = new ComplexRead11Result(0, 0);
            try {
                result = new ComplexRead11Result(
                    (double) tuple.getValue(0).getValue(),
                    (int) tuple.getValue(1).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }        
        
        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead11);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead11 operation) {
            return state.getQueryStore().getParamsComplexRead11(operation);
        }
    }

    public static class ComplexRead12Handler extends
        KuzuListOperationHandler<ComplexRead12Result, ComplexRead12> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getComplexRead12(operation);
        }


        @Override
        protected ComplexRead12Result convertSingleResult(KuzuFlatTuple tuple) {
            ComplexRead12Result result = new ComplexRead12Result(0, 0);
            try {
                result = new ComplexRead12Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (double) tuple.getValue(1).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionComplexRead12);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ComplexRead12 operation) {
            return state.getQueryStore().getParamsComplexRead12(operation);
        }
    }

    public static class SimpleRead1Handler extends
        KuzuListOperationHandler<SimpleRead1Result, SimpleRead1> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getSimpleRead1(operation);
        }


        @Override
        protected SimpleRead1Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead1Result result = new SimpleRead1Result(null, false, null);
            try {
                result = new SimpleRead1Result(
                    Date.from(tuple.getValue(0).getValue()),
                    (Boolean) tuple.getValue(1).getValue(),
                    (String) tuple.getValue(2).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead1);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead1 operation) {
            return state.getQueryStore().getParamsSimpleRead1(operation);
        }
    }

    public static class SimpleRead2Handler extends
        KuzuListOperationHandler<SimpleRead2Result, SimpleRead2> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getSimpleRead2(operation);
        }

        @Override
        protected SimpleRead2Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead2Result result = new SimpleRead2Result(0, 0, 0, 0, 0, 0);
            try {
                result = new SimpleRead2Result(
                    (double) tuple.getValue(0).getValue(),
                    (double) tuple.getValue(1).getValue(),
                    (long) tuple.getValue(2).getValue(),
                    (double) tuple.getValue(3).getValue(),
                    (double) tuple.getValue(4).getValue(),
                    (long) tuple.getValue(5).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead2);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead2 operation) {
            return state.getQueryStore().getParamsSimpleRead2(operation);
        }
    }

    public static class SimpleRead3Handler extends
        KuzuListOperationHandler<SimpleRead3Result, SimpleRead3> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getSimpleRead3(operation);
        }

        @Override
        protected SimpleRead3Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead3Result result = new SimpleRead3Result(0);
            try {
                result = new SimpleRead3Result(
                    (float) tuple.getValue(0).getValue());
            } catch (KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead3);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead3 operation) {
            return state.getQueryStore().getParamsSimpleRead3(operation);
        }
    }

    public static class SimpleRead4Handler extends
        KuzuListOperationHandler<SimpleRead4Result, SimpleRead4> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getSimpleRead4(operation);
        }

        @Override
        protected SimpleRead4Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead4Result result = new SimpleRead4Result(0, 0, 0);
            try {
                result = new SimpleRead4Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (int) tuple.getValue(1).getValue(),
                    (double) tuple.getValue(2).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead4);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead4 operation) {
            return state.getQueryStore().getParamsSimpleRead4(operation);
        }
    }

    public static class SimpleRead5Handler extends
        KuzuListOperationHandler<SimpleRead5Result, SimpleRead5> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getSimpleRead5(operation);
        }

        @Override
        protected SimpleRead5Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead5Result result = new SimpleRead5Result(0, 0, 0);
            try {
                result = new SimpleRead5Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()),
                    (int) tuple.getValue(1).getValue(),
                    (double) tuple.getValue(2).getValue());
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead5);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead5 operation) {
            return state.getQueryStore().getParamsSimpleRead5(operation);
        }
    }

    public static class SimpleRead6Handler extends
        KuzuListOperationHandler<SimpleRead6Result, SimpleRead6> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getSimpleRead6(operation);
        }

        @Override
        protected SimpleRead6Result convertSingleResult(KuzuFlatTuple tuple) {
            SimpleRead6Result result = new SimpleRead6Result(0);
            try {
                result = new SimpleRead6Result(
                    Long.parseLong((String) tuple.getValue(0).getValue()));
            } catch (NumberFormatException | KuzuObjectRefDestroyedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionSimpleRead6);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, SimpleRead6 operation) {
            return state.getQueryStore().getParamsSimpleRead6(operation);
        }
    }

    public static class Write1Handler extends
        KuzuUpdateOperationHandler<Write1> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getWrite1(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite1);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write1 operation) {
            return state.getQueryStore().getParamWrite1(operation);
        }
    }

    public static class Write2Handler extends
        KuzuUpdateOperationHandler<Write2> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getWrite2(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite2);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write2 operation) {
            return state.getQueryStore().getParamWrite2(operation);
        }
    }

    public static class Write3Handler extends
        KuzuUpdateOperationHandler<Write3> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getWrite3(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite3);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write3 operation) {
            return state.getQueryStore().getParamWrite3(operation);
        }
    }

    public static class Write4Handler extends
        KuzuUpdateOperationHandler<Write4> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getWrite4(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite4);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write4 operation) {
            return state.getQueryStore().getParamWrite4(operation);
        }
    }

    public static class Write5Handler extends
        KuzuUpdateOperationHandler<Write5> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getWrite5(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite5);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write5 operation) {
            return state.getQueryStore().getParamWrite5(operation);
        }
    }

    public static class Write6Handler extends
        KuzuUpdateOperationHandler<Write6> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getWrite6(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite6);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write6 operation) {
            return state.getQueryStore().getParamWrite6(operation);
        }
    }

    public static class Write7Handler extends
        KuzuUpdateOperationHandler<Write7> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getWrite7(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite7);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write7 operation) {
            return state.getQueryStore().getParamWrite7(operation);
        }
    }

    public static class Write8Handler extends
        KuzuUpdateOperationHandler<Write8> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getWrite8(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite8);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write8 operation) {
            return state.getQueryStore().getParamWrite8(operation);
        }
    }

    public static class Write9Handler extends
        KuzuUpdateOperationHandler<Write9> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getWrite9(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite9);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write9 operation) {
            return state.getQueryStore().getParamWrite9(operation);
        }
    }

    public static class Write10Handler extends
        KuzuUpdateOperationHandler<Write10> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getWrite10(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite10);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write10 operation) {
            return state.getQueryStore().getParamWrite10(operation);
        }
    }

    public static class Write11Handler extends
        KuzuUpdateOperationHandler<Write11> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getWrite11(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite11);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write11 operation) {
            return state.getQueryStore().getParamWrite11(operation);
        }
    }

    public static class Write12Handler extends
        KuzuUpdateOperationHandler<Write12> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getWrite12(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite12);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write12 operation) {
            return state.getQueryStore().getParamWrite12(operation);
        }
    }

    public static class Write13Handler extends
        KuzuUpdateOperationHandler<Write13> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getWrite13(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite13);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write13 operation) {
            return state.getQueryStore().getParamWrite13(operation);
        }
    }

    public static class Write14Handler extends
        KuzuUpdateOperationHandler<Write14> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write14 operation) {
            return state.getQueryStore().getWrite14(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write14 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite14);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write14 operation) {
            return state.getQueryStore().getParamWrite14(operation);
        }
    }

    public static class Write15Handler extends
        KuzuUpdateOperationHandler<Write15> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write15 operation) {
            return state.getQueryStore().getWrite15(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write15 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite15);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write15 operation) {
            return state.getQueryStore().getParamWrite15(operation);
        }
    }

    public static class Write16Handler extends
        KuzuUpdateOperationHandler<Write16> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write16 operation) {
            return state.getQueryStore().getWrite16(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write16 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite16);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write16 operation) {
            return state.getQueryStore().getParamWrite16(operation);
        }
    }

    public static class Write17Handler extends
        KuzuUpdateOperationHandler<Write17> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write17 operation) {
            return state.getQueryStore().getWrite17(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write17 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite17);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write17 operation) {
            return state.getQueryStore().getParamWrite17(operation);
        }
    }

    public static class Write18Handler extends
        KuzuUpdateOperationHandler<Write18> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write18 operation) {
            return state.getQueryStore().getWrite18(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write18 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite18);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write18 operation) {
            return state.getQueryStore().getParamWrite18(operation);
        }
    }

    public static class Write19Handler extends
        KuzuUpdateOperationHandler<Write19> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, Write19 operation) {
            return state.getQueryStore().getWrite19(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, Write19 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionWrite19);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, Write19 operation) {
            return state.getQueryStore().getParamWrite19(operation);
        }
    }

    public static class ReadWrite1Handler extends
        KuzuTransactionUpdateOperationHandler<ReadWrite1> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getReadWrite1(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionReadWrite1);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ReadWrite1 operation) {
            return state.getQueryStore().getParamReadWrite1(operation);
        }
    }

    public static class ReadWrite2Handler extends
        KuzuTransactionUpdateOperationHandler<ReadWrite2> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getReadWrite2(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionReadWrite2);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ReadWrite2 operation) {
            return state.getQueryStore().getParamReadWrite2(operation);
        }
    }

    public static class ReadWrite3Handler extends
        KuzuTransactionUpdateOperationHandler<ReadWrite3> {

        @Override
        public String getQueryString(KuzuDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getReadWrite3(operation);
        }

        @Override
        protected String getQuery(KuzuDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getQuery(QueryType.TransactionReadWrite3);
        }

        @Override
        protected Map<String, KuzuValue> getParams(KuzuDbConnectionState state, ReadWrite3 operation) {
            return state.getQueryStore().getParamReadWrite3(operation);
        }
    }
}
