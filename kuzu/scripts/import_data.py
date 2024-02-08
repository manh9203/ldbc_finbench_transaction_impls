import kuzu
import os
import sys
import logging
import shutil

base_dir = os.path.dirname(os.path.realpath(__file__))

def load_schema(conn):
    schema_file = open(os.path.join(base_dir, '..', 'schema.cypher')).read().split(';\n')

    for schema in schema_file:
        if schema == "":
            continue
        print(schema)
        print()
        logging.info(f"Loading schema {schema} \n")
        conn.execute(f"{schema}")

    logging.info("Loaded schema")

def load_dataset(conn, data_path):
    node_files = ["Account", "Company", "Loan", "Medium", "Person"]
    edge_files = ["AccountRepayLoan", "AccountTransferAccount", "AccountWithdrawAccount", "CompanyApplyLoan", "CompanyGuaranteeCompany", "CompanyInvestCompany", "CompanyOwnAccount", "LoanDepositAccount", "MediumSignInAccount", "PersonApplyLoan", "PersonGuaranteePerson", "PersonInvestCompany", "PersonOwnAccount"]

    load_schema(conn)

    extension = ".csv"
    copy_options = "(HEADER=True, DELIM='|')"

    for file in node_files:
        logging.debug(f"Loading {file}")
        conn.execute(f"""COPY {file} from '{data_path}/{file}{extension}' {copy_options}""")

    for file in edge_files:
        logging.debug(f"Loading {file}")
        conn.execute(f"""COPY {file} from '{data_path}/{file}{extension}' {copy_options}""")
    logging.info("Loaded data files")


def main():
    if len(sys.argv) < 2:
        print("Usage: scripts/import_data.py sf{x}")
        print("Where x is the scale factor")
        exit(1)
    else:
        sf = sys.argv[1]

    data_path = os.path.join(base_dir, '..', 'src', 'main', 'resources', 'data', sf, 'snapshot')
    db_path = os.path.join(base_dir, '..', 'scratch', 'db')

    shutil.rmtree(db_path, ignore_errors=True)

    db = kuzu.Database(db_path)
    conn = kuzu.Connection(db)


    logging.info("Successfully connected")

    load_dataset(conn, data_path)


if __name__ == "__main__":
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    main()