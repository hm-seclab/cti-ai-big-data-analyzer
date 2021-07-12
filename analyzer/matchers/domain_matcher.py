import pandas as pd
from string_grouper import match_strings

'''
This class represents the functionality to find similarities between domains.
'''


class DomainMatcher:

    @staticmethod
    def domain_processing():
        '''
        domain_processing finds similarities for domains within a given csv file.
        :return: a dataframe, which shows similar domains.
        '''

        print("Stepping into domain_processing.")

        domains = pd.read_csv("./data/splitted_datasets/export_domain.csv", usecols=['uuid', 'event_id', 'value'])

        matches = match_strings(domains['value'], master_id=domains['event_id'])

        diff_events_only = matches[matches.left_event_id != matches.right_event_id]

        mask = diff_events_only["similarity"] < 2.0
        df = diff_events_only[mask]

        df = df.drop(columns=["left_index", "right_index"])

        return df