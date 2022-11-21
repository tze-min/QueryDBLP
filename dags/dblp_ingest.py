from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from cassandra.cluster import Cluster
import pandas as pd
import xml.etree.ElementTree as et
import requests
import os


# set up config

hook = CassandraHook("cassandra_app")
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

default_args = {
    "owner": "tzemin",
    "depends_on_past": False,
    "email": ["tzemin.koay@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}


# define internal functions

def drop_publication_table(session):
    drop_tb = """
    DROP TABLE IF EXISTS publication;
    """
    session.execute(drop_tb)

def drop_num_publication_table(session):
    drop_tb = """
    DROP TABLE IF EXISTS num_publication;
    """
    session.execute(drop_tb)

def transform_data_for_publication(xml_path):
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
        for author in rec.findall("author"):
            if author.get("pid") == pid_to_find:
                return i
            else:
                i += 1

    def get_category(rec):
        paper_id = rec.attrib.get("key")
        if not paper_id:
            return None
        category = paper_id.split("/")[0]
        return category[:-1] if category[-1] == "s" else category

    tree = et.parse(xml_path)
    root = tree.getroot()
    records = get_fields(root)

    return records

def transform_data_for_num_publication(xml_path):
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

    tree = et.parse(xml_path)
    root = tree.getroot()
    return get_fields(root)

'''
def transform_data_for_num_publication(xml_path):
    def get_fields(root):
        person = root[0]
        pid = person.find("author").attrib.get("pid") # o/BengChinOoi

        records = []
        for r in root[1:-1]:
            rec = r[0]
            
            for coauthor in rec.findall("author"):
                attribs = {}
                attribs["pid"] = pid
                attribs["paper_key"] = rec.attrib.get("key")
                attribs["year"] = int(rec.find("year").text) if rec.find("year") is not None else None
                attribs["coauthor_pid"] = coauthor.attrib.get("pid")
                
                records.append(attribs)
        return records

    tree = et.parse(xml_path)
    root = tree.getroot()
    return get_fields(root)
'''

def create_publication_table(session, primary_key, xml_path):
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

    records = transform_data_for_publication(xml_path)
    for rec in records:
        attrib_ls = [
            rec["pid"],
            rec["category"],
            rec["year"],
            rec["position"],
            rec["paper_key"]
        ]
        session.execute(insert_statement, attrib_ls)

def create_num_publication_table(session, primary_key, xml_path):
    create_tb = f"""
    CREATE TABLE IF NOT EXISTS num_publication (
        pid text,
        year int,
        coauthor_pid text,
        paper_count int,
        PRIMARY KEY {primary_key}
    ) WITH CLUSTERING ORDER BY (paper_count DESC);
    """
    session.execute(create_tb)

    insert_data = """
    INSERT INTO num_publication (
        pid,
        year,
        coauthor_pid,
        paper_count
    ) VALUES (?,?,?,?) IF NOT EXISTS;
    """
    insert_statement = session.prepare(insert_data)

    records = transform_data_for_num_publication(xml_path)
    for rec in records:
        print(rec, "\n")
        attrib_ls = [
            rec["pid"],
            rec["year"],
            rec["coauthor_pid"],
            rec["paper_count"]
        ]
        session.execute(insert_statement, attrib_ls)


# define functions as tasks

def fetch_data_and_write_files():
    pids = pd.read_csv(AIRFLOW_HOME + "/dags/input/cs_researchers.csv")["PID"].tolist()

    for pid in pids[:1]:
        url = f"https://dblp.org/pid/{pid}.xml"
        response = requests.get(url)

        if response.status_code != 200:
            continue
        
        with open(AIRFLOW_HOME + "/dags/data/dblp_records.xml", "wb+") as f:
            f.write(response.content)
            f.close()

def insert_data_into_publication():
    session = hook.get_conn()
    drop_publication_table(session)
    create_publication_table(session, "((pid), category, position, year, paper_key)", AIRFLOW_HOME + "/dags/data/dblp_records.xml")

def insert_data_into_num_publication():
    session = hook.get_conn()
    drop_num_publication_table(session)
    create_num_publication_table(session, "((pid), year, coauthor_pid, paper_count)", AIRFLOW_HOME + "/dags/data/dblp_records.xml")

with DAG(
    "dblp_ingest",
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2022, 11, 15),
    catchup=False,
    tags=["project"]
) as dag:

    fetch_and_write_data = PythonOperator(
        task_id="fetch_and_write_data",
        python_callable=fetch_data_and_write_files
    )

    insert_into_publication = PythonOperator(
        task_id="insert_into_publication",
        python_callable=insert_data_into_publication
    )

    insert_into_num_publication = PythonOperator(
        task_id="insert_into_num_publication",
        python_callable=insert_data_into_num_publication
    )

    fetch_and_write_data >> [insert_into_publication, insert_into_num_publication]

'''
Q1:
SELECT pid, COUNT(paper_key) AS num_conf_papers
... FROM publication
... WHERE pid = '40/2499'
... AND category = 'conf'
... AND position = 3
... AND year > 2011 AND year < 2023;

Q2:
SELECT pid, COUNT(paper_key) AS num_of_pubs
    FROM publication
    WHERE pid = 'o/BengChinOoi'
    AND category IN ('journal', 'conf', 'serie', 'reference')
    AND position = 2
    AND year > 2016 AND year < 2023;

Q3:
SELECT pid, coauthor_pid, paper_count
    ... FROM num_publication
    ... WHERE pid = '40/2499'
    ... GROUP BY pid, coauthor_pid;


'''