from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from uuid import uuid1
import requests
import pandas as pd
import numpy as np
import xml.etree.cElementTree as et
import os

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

dag = DAG("dblp_data_pipeline",
    description="A DAG that fetches, processes and sends data to Cassandra databases (locally and on AstraDB)",
    schedule_interval=None,
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=["etl"]
)

def fetch_dblp_data(**context):
    task_instance = context["ti"]

    # define classes
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

    # define functions to extract fields from xml file structure of dblp's APIs
    def getFields(r):
        rec = r[0] # get to record tag (<article>, <inproceedings>, etc.) per <r>
        
        # extract fields
        paper_key = rec.attrib.get("key")
        title = rec.find("title").text if rec.find("title") is not None else None
        year = int(rec.find("year").text) if rec.find("year") is not None else None
        rec_type = rec.tag
        authors = getAuthors(rec)
        category = getCategory(rec)
        publisher = getPublisher(rec)
        position = getPosition(rec)
        ee = rec.find("ee").text if rec.find("ee") is not None else None
        url = rec.find("url").text if rec.find("url") is not None else None
        crossref = rec.find("crossref").text if rec.find("crossref") is not None else None
        mdate = rec.attrib.get("mdate")
        
        return [paper_key, title, year, rec_type, authors, category, publisher, position, ee, url, crossref, mdate]

    def getCategory(rec):
        paper_key = rec.attrib.get("key")
        if not paper_key:
            return None
        category = paper_key.split("/")[0]
        return category[:-1] if category[-1] == "s" else category

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

    def getPublisher(rec): # assuming each record only has one of the 3 tags
        tag_filter = ["booktitle", "journal", "publisher"]
        results = [rec.find(t).text if rec.find(t) is not None else None for t in tag_filter]
        for res in results:
            if res != None:
                return res

    def getPosition(rec):
        number = rec.find("number").text if rec.find("number") is not None else None
        volume = rec.find("volume").text if rec.find("volume") is not None else None
        pages = rec.find("pages").text if rec.find("pages") is not None else None
        return Position(number, volume, pages)
        
    # get list of 400 researchers we're interested in
    researchers = pd.read_csv(AIRFLOW_HOME + "/dags/cs_researchers.csv")
    pids = researchers.PID

    # extract XML data for each researcher
    for pid in pids:
        print(pid)
        url = f"https://dblp.org/pid/{pid}.xml"
        req = requests.get(url)
        df = []

        root = et.fromstring(req.content)
        print("root retrieved")
        for r in root[1:-1]:
            lst = getFields(r)
            print(lst)
            df.append(lst)

        task_instance.xcom_push("dblp_data", df)
        print("...success !")
        break

def save_to_file(**context):
    task_instance = context["ti"]
    dblp_data = task_instance.xcom_pull(
        task_id="fetch_dblp_data",
        key="dblp_data")
    dblp_data.to_csv(AIRFLOW_HOME + "/daga/data.csv")


get_data_from_dblp = PythonOperator(
    task_id="fetch_dblp_data",
    python_callable=fetch_dblp_data,
    dag=dag
)

save_data_locally = PythonOperator(
    task_id="save_to_file",
    python_callable=save_to_file,
    dag=dag
)

get_data_from_dblp >> save_data_locally