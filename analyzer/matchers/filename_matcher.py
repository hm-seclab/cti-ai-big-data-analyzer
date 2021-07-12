import pandas as pd
from string_grouper import match_strings

'''
This class represents the functionality to find similarities between filenames.
Attention -> The Filenames are not used within neo4j at the moment, because the DBMS is not able to handle
values beginning with special Characters e.g. % // ...
'''


class FilenameMatcher:

    @staticmethod
    def filename_processing():
        '''
        hostname_processing finds similarities for filenames within a given csv file.
        :return: a dataframe, which shows similar filenames.
        '''

        print("Stepping into filename_processing.")

        filenames = pd.read_csv("./data/splitted_datasets/export_filename.csv", usecols=['uuid', 'event_id', 'value'])

        matches = match_strings(filenames['value'], master_id=filenames['event_id'])

        diff_events_only = matches[matches.left_event_id != matches.right_event_id]

        mask = diff_events_only["similarity"] < 2.0
        df = diff_events_only[mask]

        df = df.drop(columns=["left_index", "right_index"])

        return df