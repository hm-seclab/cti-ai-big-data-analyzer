import pandas as pd
from string_grouper import match_strings

'''
This class represents the functionality to find similarities between hostnames.
'''


class HostnameMatcher:

    @staticmethod
    def hostname_processing():
        '''
        hostname_processing finds similarities for hostnames within a given csv file.
        :return: a dataframe, which shows similar hostnames.
        '''

        print("Stepping into hostname_processing.")

        hostnames = pd.read_csv("./data/splitted_datasets/export_hostname.csv", usecols=['uuid', 'event_id', 'value'])

        matches = match_strings(hostnames['value'], master_id=hostnames['event_id'])

        diff_events_only = matches[matches.left_event_id != matches.right_event_id]

        mask = diff_events_only["similarity"] < 2.0
        df = diff_events_only[mask]

        df = df.drop(columns=["left_index", "right_index"])

        return df