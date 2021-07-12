import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

NEO4J_URI = "bolt://localhost:" + str(os.environ.get('NEO4J_PORT'))
NEO4J_USER = str(os.environ.get('NEO4J_DBMS_USER'))
NEO4J_PASSWORD = str(os.environ.get('NEO4J_DBMS_PASSWORD'))

'''
This class represents a connection to neo4j for building a graph between similar events.
'''


class GraphBuilder:

    @staticmethod
    def build_graph(dataframe, ioc1, ioc2, ioc3):
        '''
        build_graph is the entry point to build a neo4j graph from similar events.
        :param dataframe: will be a given dataframe containing the following columns:
            left_event_id, left_value, similarity, right_event_id, right_value
        '''

        print("Starting to build events for the iocs " + str(ioc1) + ".")

        temp_df = dataframe['left_event_id'].append(dataframe['right_event_id'])
        unique_event_nodes = temp_df.drop_duplicates()
        unique_event_nodes_list = unique_event_nodes.to_list()
        list_all = dataframe.values.tolist()

        GraphBuilder.__build_nodes(unique_event_nodes_list)
        GraphBuilder.__build_relationship_for_event_and_ioc(list_all, ioc1, ioc2, ioc3)
        GraphBuilder.__build_relationship_for_ioc(list_all, ioc1, ioc2, ioc3)

        print("Done building the events for the iocs " + str(ioc1) + ".")

    @staticmethod
    def __build_nodes(unique_event_nodes_list):
        '''
        __build_nodes creates for every unique event id one node.
        :param unique_event_nodes_list: will be a list of unique event nodes.
        '''

        print("Stepping into __build_nodes.")

        unique_event_nodes_execution_commands = []

        for i in unique_event_nodes_list:
            event_node = "(e:Event {event_id: " + "'" + str(i) + "'" + "})"
            neo4j_create_statemenet = "CREATE" + event_node
            unique_event_nodes_execution_commands.append(neo4j_create_statemenet)

        GraphBuilder.__execute_transactions(unique_event_nodes_execution_commands)

    @staticmethod
    def __build_relationship_for_event_and_ioc(list_all, ioc1, ioc2, ioc3):
        '''
        __build_relationship_for_event_and_ioc creates a new relationship between an event id and an ioc.
        :param list_all: will be the dataframe transformed into a list.
        '''

        print("Stepping into __build_relationship_for_event_and_ioc.")

        url_execution_commands = []

        for i in list_all:
            match_left = "MATCH (e1:Event {event_id: " + "'" + str(i[1]) + "'" + "})"
            match_right = "MATCH (e2:Event {event_id: " + "'" + str(i[3]) + "'" + "})"
            merge_left = "MERGE (e1)-[:" + str(ioc1) + "]->(u1: " + str(ioc2) + " {" + str(ioc3) + ": " + "'" + str(i[0]) + "'" + "})"
            merge_right = "MERGE (e2)-[:" + str(ioc1) + "]->(u2: " + str(ioc2) + " {" + str(ioc3) + ": " + "'" + str(i[4]) + "'" + "})"
            neo4j_statemenet = match_left + " " + match_right + " " + merge_left + " " + merge_right
            url_execution_commands.append(neo4j_statemenet)

        GraphBuilder.__execute_transactions(url_execution_commands)

    @staticmethod
    def __build_relationship_for_ioc(list_all, ioc1, ioc2, ioc3):
        '''
        __build_relationship_for_ioc creates a new relationship between iocs from different events.
        :param list_all: will be the dataframe transformed into a list.
        '''

        print("Stepping into __build_relationship_for_ioc")

        similarity_execution_commands = []

        for i in list_all:
            match_left = "MATCH (e1:Event {event_id: " + "'" + str(
                i[1]) + "'" + "})-[:" + str(ioc1) + "]->(u1: " + str(ioc2) + " {" + str(ioc3) + ": " + "'" + str(i[0]) + "'" + "})"
            match_right = "MATCH (e2:Event {event_id: " + "'" + str(
                i[3]) + "'" + "})-[:" + str(ioc1) + "]->(u2: " + str(ioc2) + " {" + str(ioc3) + ": " + "'" + str(i[4]) + "'" + "})"
            merge = "MERGE (u1)-[:IS_SIMILAR_TO {similar: " + "'" + str(i[2]) + "'" + "}]->(u2)"
            neo4j_statemenet = match_left + " " + match_right + " " + merge
            similarity_execution_commands.append(neo4j_statemenet)

        GraphBuilder.__execute_transactions(similarity_execution_commands)

    @staticmethod
    def __execute_transactions(commands):
        '''
        __execute_transactions execute given commands in the neo4j instance.
        :param commands: will be the given commands
        '''
        data_base_connection = GraphDatabase.driver(uri=NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        session = data_base_connection.session()

        for i in commands:
            session.run(i)

        session.close()
