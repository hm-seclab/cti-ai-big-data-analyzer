import os
import time
from collections import defaultdict

import pandas as pd
import pypdns
from dns import reversename, resolver
from dotenv import load_dotenv
from ipwhois import IPWhois
from tldextract import tldextract

load_dotenv()
'''
This class represents the functionality to find similarities between ip addresses.
'''
USE_ONLY_A_SAMPLE_FOR_IP_ADDRESSES = os.environ.get('USE_ONLY_A_SAMPLE_FOR_IP_ADDRESSES').lower() in ('true', '1', 't')
CIRCL_PDNS_USERNAME = os.environ.get('CIRCL_PDNS_USERNAME')
CIRCL_PDNS_PASSWORD = os.environ.get('CIRCL_PDNS_PASSWORD')


class IpMatcher:

    @staticmethod
    def ip_address_processing():
        '''
        ip_address_processing finds similarities for ip addresses within a given csv file.
        :return: a dataframe, which shows similar ip addresses.
        '''

        print("Stepping into ip_address_processing.")

        misp_df = pd.read_csv("./data/splitted_datasets/export_ip.csv")

        if USE_ONLY_A_SAMPLE_FOR_IP_ADDRESSES:
            print("Using only a sample of 100 of the available ip addresses for collecting the metadata.")
            misp_df = misp_df.sample(frac=.25)
            misp_df = misp_df.head(100)

        ip_list = misp_df['value'].tolist()

        ip_domain_dict = IpMatcher.__ip_address_reverse_lookup(ip_list)
        ip_pdns_dict = IpMatcher.__ip_address_passive_dns_lookup(ip_list)
        ip_asn_registry_dict, ip_asn_dict, ip_asn_cidr_dict, ip_asn_country_code_dict = IpMatcher.__ip_address_asn_lookup(
            ip_list)

        ip_df = pd.DataFrame({'domain': pd.Series(ip_domain_dict),
                              'asn_registry': pd.Series(ip_asn_registry_dict),
                              'asn': pd.Series(ip_asn_dict),
                              'asn_cidr': pd.Series(ip_asn_cidr_dict),
                              'country_code': pd.Series(ip_asn_country_code_dict),
                              'pdns': pd.Series(ip_pdns_dict)})

        filtered_df = ip_df[ip_df[['domain', 'asn_registry', 'asn', 'asn_cidr', 'country_code']].notnull().all(1)]
        filtered_df.reset_index(level=0, inplace=True)
        filtered_df = filtered_df.rename(columns={'index': 'value'}, inplace=False)
        joined_df = misp_df.join(filtered_df.set_index('value'), on='value')

        df1 = joined_df
        df2 = joined_df

        df_merged = df1.merge(df2, how='cross')
        df_merged['check_string'] = df_merged.apply(lambda row: ''.join(sorted([row['uuid_x'], row['uuid_y']])), axis=1)
        df_merged = df_merged.drop_duplicates('check_string')

        df_merged['similarity'] = df_merged.apply(
            lambda row: IpMatcher.__compare_ip_metadata(row['domain_x'], row['domain_y'],
                                                       row['asn_x'], row['asn_y'],
                                                       row['asn_registry_x'],
                                                       row['asn_registry_y'],
                                                       row['asn_cidr_x'], row['asn_cidr_y'],
                                                       row['country_code_x'],
                                                       row['country_code_y'],
                                                       row['pdns_x'], row['pdns_y']), axis=1)

        # mask = df_merged['similarity'] > 0.4

        df = df_merged.drop(
            ['uuid_x', 'domain_x', 'asn_registry_x', 'asn_x', 'asn_cidr_x', 'country_code_x', 'pdns_x', 'uuid_y',
             'domain_y', 'asn_registry_y', 'asn_y', 'asn_cidr_y', 'country_code_y', 'pdns_y', 'check_string'], axis=1)

        df.columns = ['left_event_id', 'left_value', 'right_event_id', 'right_value', 'similarity']

        return df

    @staticmethod
    def __ip_address_reverse_lookup(ip_list):
        '''
        __ip_address_reverse_lookup does a simple reverse lookup for a given list of ip addresses.
        :param ip_list: will be the given ip list
        :return: will be a ip - domain dict
        '''

        print("Stepping into __ip_address_reverse_lookup.")

        def_dict_reverse = defaultdict(list)

        for element in ip_list:
            try:
                rev_name = reversename.from_address(str(element))
                reversed_dns = str(resolver.resolve(rev_name, "PTR")[0])
                result = tldextract.extract(reversed_dns)
                domain = result.domain

                if element not in def_dict_reverse:
                    def_dict_reverse[element] = domain
                else:
                    def_dict_reverse[element].append(domain)
            except:
                continue
        return dict(def_dict_reverse)

    @staticmethod
    def __ip_address_passive_dns_lookup(ip_list):
        '''
        __ip_address_passive_dns_lookup does a passive dns lookup at circl for a given list of ip addresses.
        :param ip_list: will be the given ip list
        :return: will be a ip - pdns dict
        '''

        print("Stepping into __ip_address_passive_dns_lookup.")

        def_dict_pdns = defaultdict(list)

        request = pypdns.PyPDNS(
            basic_auth=(CIRCL_PDNS_USERNAME, CIRCL_PDNS_PASSWORD))
        count = 0

        for element in ip_list:

            count += 1
            print(count)

            time.sleep(2)

            pdns_result = request.query(str(element))

            if len(pdns_result) == 0:
                continue

            rdata_list = []

            for pdns_result_element in pdns_result:
                rdata = pdns_result_element['rdata']
                rdata_list.append(rdata)

            if element not in def_dict_pdns:
                def_dict_pdns[element] = rdata_list

        return dict(def_dict_pdns)

    @staticmethod
    def __ip_address_asn_lookup(ip_list):
        '''
        __ip_address_asn_lookup does a asn lookup for selected attributes for a given list of ip addresses.
        :param ip_list: will be the given ip list
        :return: will be various dicts, containing the ip asn information
        '''

        print("Stepping into __ip_address_asn_lookup.")

        def_dict_asn_registry = defaultdict(list)
        def_dict_asn = defaultdict(list)
        def_dict_asn_cidr = defaultdict(list)
        def_dict_asn_country_code = defaultdict(list)

        count = 0

        for element in ip_list:
            try:
                query = IPWhois(str(element))
                asn_results = query.lookup_rdap(depth=7, rate_limit_timeout=120)
                asn_registry = asn_results.get('asn_registry')
                asn = asn_results.get('asn')
                asn_cidr = asn_results.get('asn_cidr')
                asn_country_code = asn_results.get('asn_country_code')
                asn_date = asn_results.get('asn_date')

                def_dict_asn_registry[element] = asn_registry
                def_dict_asn[element] = asn
                def_dict_asn_cidr[element] = asn_cidr
                def_dict_asn_country_code[element] = asn_country_code
                count += 1
                print(count)
            except:
                continue

        return dict(def_dict_asn_registry), dict(def_dict_asn), dict(def_dict_asn_cidr), dict(
            def_dict_asn_country_code)

    @staticmethod
    def __compare_ip_metadata(domain_x, domain_y,
                              asn_x, asn_y,
                              asn_registry_x, asn_registry_y,
                              asn_cidr_x, asn_cidr_y,
                              country_code_x, country_code_y,
                              pdns_x, pdns_y):
        '''
        __compare_ip_metadata does a comparision of all relevant ip address metadata
        :param domain_x: will be the first domain value
        :param domain_y: will be the second domain value
        :param asn_x: will be the first asn value
        :param asn_y: will be the second asn value
        :param asn_registry_x: will be the first asn registry value
        :param asn_registry_y: will be the second asn registry value
        :param asn_cidr_x: will be the first asn cidr value
        :param asn_cidr_y: will be the second asn cidr value
        :param country_code_x: will be the first country code value
        :param country_code_y: will be the second country code value
        :param pdns_x: will be the first passive dns value as a list
        :param pdns_y: will be the second passive dns value as a list
        :return: will return a score, how similar two ip address
        '''

        domain_value = 0
        asn_registry_value = 0
        asn_cidr_value = 0
        country_code_value = 0
        pdns_value = 0
        asn_value = 0

        if domain_x == domain_y:
            domain_value = 1

        if asn_registry_x == asn_registry_y:
            asn_registry_value = 1

        if asn_cidr_x == asn_cidr_y:
            asn_cidr_value = 1

        if country_code_x == country_code_y:
            country_code_value = 1

        if asn_x == asn_y:
            asn_value = 1

        if type(pdns_x) is list and type(pdns_y) is list:
            for element in pdns_x:
                if element in pdns_y:
                    pdns_value = 1

        tmp = int(domain_value) + int(asn_registry_value) + int(asn_cidr_value) + int(country_code_value) + int(
            pdns_value) + int(asn_value)

        score = tmp / 6

        return score