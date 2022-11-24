from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import xml.etree.ElementTree as et
import requests
import os
from pathlib import Path


# set up config

hook = CassandraHook("cassandra_app")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

cloud_config = {'secure_connect_bundle': AIRFLOW_HOME + '/dags/secure-connect-dblp.zip'}
auth_provider = PlainTextAuthProvider('ID', 'TOKEN')

default_args = {
    "owner": "tzemin",
    "depends_on_past": False,
    "email": ["tzemin.koay@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}


# update local database: define internal functions

def drop_publication_table(session):
    drop_tb = """
    DROP TABLE IF EXISTS publication;
    """
    session.execute(drop_tb)

def transform_data_for_publication(xml_folder_path):
    """Extract data from xml file and transform into format suited for the publication table"""

    def get_fields(root):
        person = root[0]
        pid = person.find("author").attrib.get("pid") # o/BengChinOoi

        records = [] # we'll populate this list with all records (ie publications) of the same author found in root
        for r in root[1:-1]:
            attribs = {}
            attribs["pid"] = pid

            rec = r[0] # get to record tag (<article>, <inproceedings>, etc.) per <r>
            attribs["category"] = get_category(rec)
            attribs["year"] = int(rec.find("year").text) if rec.find("year") is not None else None
            attribs["position"] = get_position(attribs["pid"], rec)
            attribs["paper_key"] = rec.attrib.get("key")

            records.append(attribs)
        
        return records

    def get_position(pid_to_find, rec) -> int:
        "Get position of author (identified by pid_to_find) within list of authors of the record, rec. E.g. 1st, 2nd author"
        i = 1
        authors = rec.findall("author")
        if len(authors) == 0:
            return 0            
        for author in authors:
            if author.get("pid") == pid_to_find:
                return i
            else:
                i += 1

    def get_category(rec):
        paper_id = rec.attrib.get("key")
        if not paper_id:
            return None
        category = paper_id.split("/")[0]
        return category

    paths = Path(xml_folder_path).glob("*.xml")
    records = []
    for xml_path in paths:
        print("transforming", str(xml_path))
        tree = et.parse(xml_path)
        root = tree.getroot()
        records.extend(get_fields(root))

    return records

def create_publication_table(session, primary_key, xml_folder_path):
    create_tb = f"""
    CREATE TABLE IF NOT EXISTS publication (
        pid text,
        category text,
        year int,
        position int,
        paper_key text,
        PRIMARY KEY {primary_key}
    );
    """
    session.execute(create_tb)

    insert_data = """
    INSERT INTO publication (
        pid,
        category,
        year,
        position,
        paper_key
    ) VALUES (?,?,?,?,?) IF NOT EXISTS;
    """
    insert_statement = session.prepare(insert_data)

    records = transform_data_for_publication(xml_folder_path)
    for rec in records:
        attrib_ls = [
            rec["pid"],
            rec["category"],
            rec["year"],
            rec["position"],
            rec["paper_key"]
        ]
        session.execute(insert_statement, attrib_ls)

def drop_collaboration_table(session):
    drop_tb = """
    DROP TABLE IF EXISTS collaboration;
    """
    session.execute(drop_tb)

def transform_data_for_collaboration(xml_folder_path):
    def get_fields(root):
        person = root[0]
        pid = person.find("author").attrib.get("pid") # o/BengChinOoi

        records_dict = {}
        for r in root[1:-1]:
            rec = r[0]
            year = int(rec.find("year").text) if rec.find("year") is not None else None
            if year not in records_dict.keys():
                records_dict[year] = {}

            for coauthor in rec.findall("author"):
                coauthor_pid = coauthor.attrib.get("pid")
                records_dict[year][coauthor_pid] = records_dict[year].get(coauthor_pid, 0) + 1

        # flatten nested dictionary (records_dict) into a list of dictionaries (records)
        records = []
        for year in records_dict.keys():
            attribs = {}
            records_in_year = records_dict[year]
            for coauthor_pid in records_in_year.keys():
                attribs["pid"] = pid
                attribs["coauthor_pid"] = coauthor_pid
                attribs["year"] = year
                attribs["paper_count"] = records_in_year[coauthor_pid]
            records.append(attribs)

        return records

    paths = Path(xml_folder_path).glob("*.xml")
    records = []
    for xml_path in paths:
        tree = et.parse(xml_path)
        root = tree.getroot()
        records.extend(get_fields(root))

    return records

def create_collaboration_table(session, primary_key, xml_folder_path):
    create_tb = f"""
    CREATE TABLE IF NOT EXISTS collaboration (
        pid text,
        year int,
        coauthor_pid text,
        paper_count int,
        PRIMARY KEY {primary_key}
    ) WITH CLUSTERING ORDER BY (year DESC, coauthor_pid ASC, paper_count DESC);
    """
    session.execute(create_tb)

    insert_data = """
    INSERT INTO collaboration (
        pid,
        year,
        coauthor_pid,
        paper_count
    ) VALUES (?,?,?,?) IF NOT EXISTS;
    """
    insert_statement = session.prepare(insert_data)

    records = transform_data_for_collaboration(xml_folder_path)
    for rec in records:
        attrib_ls = [
            rec["pid"],
            rec["year"],
            rec["coauthor_pid"],
            rec["paper_count"]
        ]
        session.execute(insert_statement, attrib_ls)


# update local database: define internal functions

def fetch_data_and_write_files():
    pids = pd.read_csv(AIRFLOW_HOME + "/dags/input/cs_researchers.csv")["PID"].tolist()

    for i in range(len(pids)): # range(3):
        pid = pids[i]
        url = f"https://dblp.org/pid/{pid}.xml"
        response = requests.get(url)

        if response.status_code != 200:
            continue

        with open(AIRFLOW_HOME + f"/dags/data/dblp_records_{i}.xml", "wb+") as f:
            f.write(response.content)
            f.close()

def insert_data_into_publication():
    session = hook.get_conn()
    drop_publication_table(session)
    create_publication_table(session, "((pid), category, position, year, paper_key)", AIRFLOW_HOME + "/dags/data")

def insert_data_into_collaboration():
    session = hook.get_conn()
    drop_collaboration_table(session)
    create_collaboration_table(session, "((pid), year, coauthor_pid, paper_count)", AIRFLOW_HOME + "/dags/data")


# query local database: define functions as tasks

def query_publication_for_q1(session_local, session_cloud):
    query_data = """
    SELECT pid, COUNT(paper_key) AS num_conf_papers
        FROM publication
        WHERE pid = '40/2499'
        AND category = 'conf'
        AND position = 3
        AND year > 2011 AND year < 2023;
    """
    rows = session_local.execute(query_data)

    insert_data = """
    INSERT INTO q1 (
        pid,
        num_conf_papers
    ) VALUES (?,?) IF NOT EXISTS;
    """
    insert_statement = session_cloud.prepare(insert_data)

    for (pid, num_conf_papers) in rows:
        session_cloud.execute(insert_statement, [pid, num_conf_papers])

def query_publication_for_q2(session_local, session_cloud):
    query_data = """
    SELECT pid, COUNT(paper_key) AS num_pubs
        FROM publication
        WHERE pid = 'o/BengChinOoi'
        AND category IN ('journals', 'conf', 'series', 'reference', 'books')
        AND position = 2
        AND year > 2016 AND year < 2023;
    """
    rows = session_local.execute(query_data)

    insert_data = """
    INSERT INTO q2 (
        pid,
        num_pubs
    ) VALUES (?,?)
    """
    insert_statement = session_cloud.prepare(insert_data)

    for (pid, num_pubs) in rows:
        session_cloud.execute(insert_statement, [pid, num_pubs])

def query_collaboration_for_q3(session_local, session_cloud):
    query_data = """
    SELECT pid, coauthor_pid, paper_count
        FROM collaboration
        WHERE pid = '40/2499'
        GROUP BY pid, year, coauthor_pid;
    """
    rows = session_local.execute(query_data)

    insert_data = """
    INSERT INTO q3 (
        pid,
        coauthor_pid,
        paper_count
    ) VALUES (?,?,?)    
    """
    insert_statement = session_cloud.prepare(insert_data)

    for (pid, coauthor_pid, paper_count) in rows:
        session_cloud.execute(insert_statement, [pid, coauthor_pid, paper_count])

def query_collaboration_for_q4(session_local, session_cloud):
    query_data = """
    SELECT coauthor_pid, year, MAX(paper_count) AS paper_count
        FROM collaboration
        WHERE pid = 'o/BengChinOoi'
        AND year = 2020
        GROUP BY pid, year, coauthor_pid;
    """
    rows = session_local.execute(query_data)

    insert_data = """
    INSERT INTO q4 (
        coauthor_pid,
        year,
        paper_count
    ) VALUES (?,?,?)    
    """
    insert_statement = session_cloud.prepare(insert_data)

    for (coauthor_pid, year, paper_count) in rows:
        session_cloud.execute(insert_statement, [coauthor_pid, year, paper_count])


# update cloud database: define functions as tasks

def checkdbconnection():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    row = session.execute("SELECT release_version FROM system.local").one()
    if row:
        print("Cassandra version", row[0])
    else:
        print("An error occurred.")

def updateclouddb_q1():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session_cloud = cluster.connect()
    session_cloud.set_keyspace("dblp")

    session_local = hook.get_conn()
    session_local.set_keyspace("dblp")
    query_publication_for_q1(session_local, session_cloud)

def updateclouddb_q2():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session_cloud = cluster.connect()
    session_cloud.set_keyspace("dblp")

    session_local = hook.get_conn()
    session_local.set_keyspace("dblp")
    query_publication_for_q2(session_local, session_cloud)

def updateclouddb_q3():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session_cloud = cluster.connect()
    session_cloud.set_keyspace("dblp")

    session_local = hook.get_conn()
    session_local.set_keyspace("dblp")
    query_collaboration_for_q3(session_local, session_cloud)

def updateclouddb_q4():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session_cloud = cluster.connect()
    session_cloud.set_keyspace("dblp")

    session_local = hook.get_conn()
    session_local.set_keyspace("dblp")
    query_collaboration_for_q4(session_local, session_cloud)

def queryclouddbdata():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    session.set_keyspace("publication")
    session.execute("SELECT COUNT(*) FROM publication;").one()[0]


with DAG(
    "dblp_ingest",
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2022, 11, 15),
    catchup=False,
    tags=["project"]
) as dag:

    fetch_and_write_data = PythonOperator(
        task_id="local_fetch_and_write_data",
        python_callable=fetch_data_and_write_files
    )

    insert_into_publication = PythonOperator(
        task_id="local_insert_into_publication",
        python_callable=insert_data_into_publication
    )

    insert_into_collaboration = PythonOperator(
        task_id="local_insert_into_collaboration",
        python_callable=insert_data_into_collaboration
    )

    check_clouddb_connection = PythonOperator(
        task_id="check_clouddb_connection",
        python_callable=checkdbconnection
    )

    update_clouddb_publication_q1 = PythonOperator(
        task_id="update_clouddb_publication_q1",
        python_callable=updateclouddb_q1
    )

    update_clouddb_publication_q2 = PythonOperator(
        task_id="update_clouddb_publication_q2",
        python_callable=updateclouddb_q2
    )

    update_clouddb_collaboration_q3 = PythonOperator(
        task_id="update_clouddb_collaboration_q3",
        python_callable=updateclouddb_q3
    )

    update_clouddb_collaboration_q4 = PythonOperator(
        task_id="update_clouddb_collaboration_q4",
        python_callable=updateclouddb_q4
    )

    fetch_and_write_data >> [insert_into_publication, insert_into_collaboration] >> check_clouddb_connection >> [update_clouddb_publication_q1, update_clouddb_publication_q2, update_clouddb_collaboration_q3, update_clouddb_collaboration_q4]