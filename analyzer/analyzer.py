import os

from dotenv import load_dotenv

from analyzer.graph_builder.graph_builder import GraphBuilder
from analyzer.matchers.domain_matcher import DomainMatcher
from analyzer.matchers.hostname_matcher import HostnameMatcher
from analyzer.matchers.ip_matcher import IpMatcher
from helper.dataset_splitter import DataSetSplitter
import pandas as pd

load_dotenv()
'''
This class represents the analyzer, in which several functions are defined to find similarities
between events.
'''

LOAD_NEW_MISP_EVENTS = os.environ.get('LOAD_NEW_MISP_EVENTS').lower() in ('true', '1', 't')
USE_TEST_DATA = os.environ.get('USE_TEST_DATA').lower() in ('true', '1', 't')


class Analyzer:

    @staticmethod
    def find_similarities():
        '''
        find_similarities is the entry point to start the similarity finding.
        '''

        print("Starting the similarity finding process.")

        if LOAD_NEW_MISP_EVENTS:
            print("Loading new events from MISP. This may take a while.")
            os.system('python3 ./helper/get_csv.py -f misp_events.csv')

        data_set_splitter = DataSetSplitter()
        data_set_splitter.split_datasets("./data")

        if USE_TEST_DATA:
            print("Using test data from /data/test_data")
            dataframe1 = pd.read_csv("./data/test_data/ip_test_data.csv")
            dataframe2 = pd.read_csv("./data/test_data/domain_test_data.csv")
            dataframe3 = pd.read_csv("./data/test_data/hostname_test_data.csv")

            cols = dataframe1.columns.tolist()

            cols[1], cols[0], cols[2], cols[3], cols[4] = cols[0], cols[1], cols[2], cols[4], cols[3]
        else:
            dataframe1 = IpMatcher.ip_address_processing()
            dataframe2 = DomainMatcher.domain_processing()
            dataframe3 = HostnameMatcher.hostname_processing()

            cols = dataframe1.columns.tolist()

            cols[1], cols[0], cols[2], cols[3], cols[4] = cols[0], cols[1], cols[4], cols[2], cols[3]

        dataframe2 = dataframe2.loc[:, ~dataframe2.columns.str.contains('^Unnamed')]
        dataframe3 = dataframe3.loc[:, ~dataframe3.columns.str.contains('^Unnamed')]

        dataframe1 = dataframe1[cols]
        dataframe1 = dataframe1[dataframe1.similarity > 0.5]

        graph_builder = GraphBuilder()
        graph_builder.build_graph(dataframe1, "IP", "Ip", "ip")
        graph_builder.build_graph(dataframe2, "URL", "Url", "url")
        graph_builder.build_graph(dataframe3, "HOSTNAME", "Hostname", "hostname")

        print("The similarity finding process has been finished successfully.\n" +
              "You can access the graph e.g. by typing one of the following "
              "commands in the cypher console inside your neo4j DBMS instance:\n" +
              "Match (n)-[r]->(m) Return n,r,m")
