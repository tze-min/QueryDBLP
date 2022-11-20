from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import xml.etree.ElementTree as ET
import requests
import os

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

def extract_from_xml(file_to_process):
    # define classes
    '''
    class Author(object):
        def __init__(self, name, orcid, pid):
            self.name = name
            self.orcid = orcid
            self.pid = pid

    class Position(object):
        def __init__(self, number, volume, pages):
            self.number = number
            self.volume = volume
            self.pages = pages
    '''
    # define functions to extract fields from xml file structure of dblp's APIs
    def getFields(r):
        rec = r[0] # get to record tag (<article>, <inproceedings>, etc.) per <r>
        pub_attribs = {}
        
        # extract fields
        pub_attribs["paper_key"] = rec.attrib.get("key")
        pub_attribs["title"] = rec.find("title").text if rec.find("title") is not None else None
        pub_attribs["year"] = int(rec.find("year").text) if rec.find("year") is not None else None
        pub_attribs["rec_type"] = rec.tag
        #pub_attribs["authors"] = getAuthors(rec)
        pub_attribs["category"] = getCategory(rec)
        pub_attribs["publisher"] = getPublisher(rec)
        #pub_attribs["position"] = getPosition(rec)
        pub_attribs["ee"] = rec.find("ee").text if rec.find("ee") is not None else None
        pub_attribs["url"] = rec.find("url").text if rec.find("url") is not None else None
        pub_attribs["crossref"] = rec.find("crossref").text if rec.find("crossref") is not None else None
        pub_attribs["mdate"] = rec.attrib.get("mdate")
        
        return pub_attribs

    def getCategory(rec):
        paper_key = rec.attrib.get("key")
        if not paper_key:
            return None
        category = paper_key.split("/")[0]
        return category[:-1] if category[-1] == "s" else category

    '''
    def getAuthors(rec):
        authors = dict()
        i = 1
        for author in rec.findall("author"):
            name = author.text
            orcid = author.get("orcid")
            pid = author.get("pid")
            authors[i] = Author(name, orcid, pid)
            i += 1
        return authors
    '''
    def getPublisher(rec): # assuming each record only has one of the 3 tags
        tag_filter = ["booktitle", "journal", "publisher"]
        results = [rec.find(t).text if rec.find(t) is not None else None for t in tag_filter]
        for res in results:
            if res != None:
                return res
    '''
    def getPosition(rec):
        number = rec.find("number").text if rec.find("number") is not None else None
        volume = rec.find("volume").text if rec.find("volume") is not None else None
        pages = rec.find("pages").text if rec.find("pages") is not None else None
        return Position(number, volume, pages)
    '''
    # parse an xml file
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    pub_list = []

    for r in root[1:-1]:
        pub_list.append(getFields(r))
    return pub_list

def drop_table(session):
    drop_tb = """
    DROP TABLE IF EXISTS publication;
    """
    session.execute(drop_tb)

def create_table_with_data(session, primarykey_setting, raw_xml_path):
    create_tb = f"""
    CREATE TABLE IF NOT EXISTS publication (
        paper_key text PRIMARY KEY,
        title text,
        year int,
        type text,
        category text,
        publisher text,
        ee text,
        url text,
        crossref text,
        mdate date
    );
    """
    session.execute(create_tb)

    insert_data = """
    INSERT INTO publication (
        paper_key,
        title,
        year,
        type,
        category,
        publisher,
        ee,
        url,
        crossref,
        mdate
    ) VALUES (?,?,?,?,?,?,?,?,?,?);
    """
    insert_statement = session.prepare(insert_data)

    pub_ls = extract_from_xml(raw_xml_path)
    for pub in pub_ls:
        attribs_ls = [
            pub["paper_key"],
            pub["title"],
            pub["year"],
            pub["rec_type"],
            #pub["authors"],
            pub["category"],
            pub["publisher"],
            #pub["position"],
            pub["ee"],
            pub["url"],
            pub['crossref'],
            pub["mdate"]
        ]
        session.execute(insert_statement, attribs_ls)

cloud_config = {'secure_connect_bundle': AIRFLOW_HOME + '/dags/secure-connect-dblp.zip'}
auth_provider = PlainTextAuthProvider('ID', 'TOKEN')

def tryout():
    print("trying this out!")

def checkdbconnection():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    row = session.execute("SELECT release_version FROM system.local").one()
    if row:
        print("Cassandra version", row[0])
    else:
        print("An error occurred.")

def updateclouddbdata():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    session.set_keyspace("publication")

    drop_table(session)
    url = "https://dblp.org/pid/o/BengChinOoi.xml" # insert later!
    response = requests.get(url)
    with open(AIRFLOW_HOME + "/dags/xmlfile.xml", "wb+") as f:
        f.write(response.content)
        f.close()
    pub_ls = create_table_with_data(session, "paper_key", AIRFLOW_HOME + "/dags/xmlfile.xml")

def queryclouddbdata():
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, protocol_version=4)
    session = cluster.connect()
    session.set_keyspace("publication")
    result = session.execute("SELECT COUNT(*) FROM publication;").one()[0]

with DAG(
    "update_astra_with_data",
    default_args=default_args,
    description="update Astra DB with dblp data pf one author's publication record",
    schedule_interval=None, #timedelta(seconds=300),
    start_date=datetime(2022, 11, 15),
    catchup=False,
    tags=["dblp"]
) as dag:
    try_out = PythonOperator(
        task_id="try_out",
        python_callable=tryout
    )

    check_dbconnection = PythonOperator(
        task_id="check_connection",
        python_callable=checkdbconnection
    )

    update_clouddb_data = PythonOperator(
        task_id="update_data",
        python_callable=updateclouddbdata
    )

    query_clouddb_data = PythonOperator(
        task_id="query_data",
        python_callable=queryclouddbdata
    )

    try_out >> check_dbconnection >> update_clouddb_data >> query_clouddb_data