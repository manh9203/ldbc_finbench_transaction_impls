import kuzu
import os
import logging
from datetime import datetime

base_dir = os.path.dirname(os.path.realpath(__file__))
data_path = os.path.join(base_dir, '..', 'src', 'main', 'resources', 'data', 'snapshot')
db_path = os.path.join(base_dir, '..', 'scratch', 'db')


def main():
    db = kuzu.Database(db_path)

    conn = kuzu.Connection(db)

    query_string = open(os.path.join(base_dir, '..', 'queries', 'transaction-complex-read-4.cypher')).read()
    query = conn.prepare(query_string)
    params = {
        'id1': 232499155396735787,
        'id2': 173107385554967308,
        'startTime': datetime.strptime("Fri Jul 23 02:10:16 EDT 2021", "%a %b %d %H:%M:%S %Z %Y"),
        'endTime': datetime.strptime("Mon Nov 28 21:52:22 EST 2022", "%a %b %d %H:%M:%S %Z %Y")
    }
    results = conn.execute(query, params)
    if (results.has_next()):
        print(results.get_next())
    print(datetime.strptime("Fri Jul 23 02:10:16 EDT 2021", "%a %b %d %H:%M:%S %Z %Y"))
    print(datetime.strptime("Mon Nov 28 21:52:22 EST 2022", "%a %b %d %H:%M:%S %Z %Y"))


if __name__ == "__main__":
    logging.basicConfig(format='%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
    main()