import pandas as pd


class DataSetSplitter:

    @staticmethod
    def split_datasets(path_to_dataset):
        df = pd.read_csv(path_to_dataset + "/misp_events.csv", dtype={'value': 'string'})

        df_ip_src = df[ df['type'] == 'ip-src']
        df_ip_dst =df[ df['type'] == 'ip-dst']
        df_ip = [df_ip_src, df_ip_dst]
        df_ip = pd.concat(df_ip)

        df_ip.to_csv (path_to_dataset + '/splitted_datasets/export_ip.csv', index = False, header=True, columns = ['uuid', 'event_id', 'value'] )

        df_domain = df[df['type'] == 'domain']
        df_domain.to_csv (path_to_dataset + '/splitted_datasets/export_domain.csv', index = False, header=True,columns = ['uuid', 'event_id', 'value'])

        df_hostname = df[df['type'] == 'hostname']
        df_hostname.to_csv (path_to_dataset + '/splitted_datasets/export_hostname.csv', index = False, header=True, columns = ['uuid', 'event_id', 'value'])