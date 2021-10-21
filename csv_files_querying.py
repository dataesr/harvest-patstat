from datetime import datetime as dt
import glob
import pandas as pd


# Chargement des données multi-CSV et requêtes
def multi_csv_files_querying(files_directory: str, chunk_query, dict_param_load_csv: dict) -> pd.DataFrame:
    files_list = glob.glob(files_directory + "/*.csv")
    df = list(map(lambda a: csv_file_querying(a, chunk_query, dict_param_load_csv), files_list))
    ending_dataframe = pd.concat(df)
    print("Results tables concatenation")
    return ending_dataframe


def csv_file_querying(csv_file: str, chunk_query, dict_param_load_csv: dict) -> pd.DataFrame:
    df_chunk = pd.read_csv(csv_file, sep=dict_param_load_csv.get('sep'), chunksize=dict_param_load_csv.get('chunksize'),
                           usecols=dict_param_load_csv.get('usecols'), dtype=dict_param_load_csv.get('dtype'))
    print(f"query beginning {csv_file}: ", dt.now())
    chunk_result_list = list(map(lambda chunk: chunk_query(chunk), df_chunk))
    print(f"end of query {csv_file} at : ", dt.now())
    dataframe_result = pd.concat(chunk_result_list)

    return dataframe_result


def filtering(files_directory: str, pat: pd.DataFrame, colfilter: str, dict_param_load_csv: dict) -> pd.DataFrame:
    """
    This functions filters data during the pd.read_csv process.
    Usually, it keeps only the IDs that are found in a different df

    :param files_directory: str corresponding to the directory where the files are
    :param pat: df where the filtering info are
    :param colfilter: str corresponding to the column which serves as a filter
    :param dict_param_load_csv: dictionary with loading parameters
    :return: df with filtered values
    """
    def lect_patstat_table(chunk):
        query = chunk[chunk[colfilter].isin(pat[colfilter])]

        return query

    table = multi_csv_files_querying(files_directory, lect_patstat_table, dict_param_load_csv)

    return table
