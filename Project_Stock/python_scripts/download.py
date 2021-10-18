import pandas as pd
from os import path, mkdir, getcwd as get_current_directory
from pandas_datareader import DataReader
from joblib import Parallel, delayed
from tqdm import tqdm

def download_stock_data(
    stock_label:str,
    end_date:str,
    start_date:str,
    data_source:str = "stooq"
) -> pd.DataFrame:
    stock_data = None
    try:
        stock_data = DataReader(
            name = stock_label,
            data_source = data_source,
            start = start_date,
            end = end_date
        )
    except (IOError, KeyError):
        print("Failed to read data: {}.".format(stock_label))
    return stock_data

def download_stock_data_to_folder(
    folder_path:str,
    stock_label:str,
    end_date:str,
    start_date:str,
    stock_data_prefix:str = "stock_data",
    data_source:str = "stooq"
) -> int:
    
    save_path = path.join(
        folder_path,
        "{prefix}_{label}.parq".format(
            prefix = stock_data_prefix, 
            label = stock_label.lower()
        )
    )
    
    if path.exists(save_path):
        return 1
    
    stock_data = download_stock_data(
        stock_label=stock_label,
        end_date=end_date,
        start_date=start_date,
        data_source=data_source
    )
    
    if stock_data is not None:
        stock_data.to_parquet(save_path, compression = "GZIP")
    
    return 1

def download_stock_data_in_parallel(
    stock_labels_list:list,
    root_folder_path:str,
    save_folder_name:str,
    end_date:str,
    start_date:str,
    stock_data_prefix:str = "stock_data",
    data_source:str = "stooq",
    n_jobs:int=12,
):
    if root_folder_path is None:
        root_folder_path = get_current_directory()
    
    assert path.exists(root_folder_path)
    
    save_folder_path = path.join(
        root_folder_path,
        save_folder_name
    )
    
    if not path.exists(save_folder_path):
        mkdir(save_folder_path)
    
    date_folder_path = path.join(
        save_folder_path,
        "{date_from}_to_{date_to}".format(
            date_from=str(start_date).replace("-",""),
            date_to=str(end_date).replace("-","")
        )
    )
    
    if not path.exists(date_folder_path):
        mkdir(date_folder_path)
    
    Parallel(n_jobs=n_jobs)(
        delayed(download_stock_data_to_folder)(
            folder_path=date_folder_path,
            stock_label=stock_label,
            end_date=end_date,
            start_date=start_date,
            stock_data_prefix=stock_data_prefix,
            data_source=data_source
        )
        for stock_label in tqdm(stock_labels_list)
    )
    
    return 1