import pandas as pd
from os import path, listdir, getcwd as get_current_directory

DEFAULT_STOCK_DOWNLOAD_FOLDER_NAME = "stock_data_download"

def load_stock_data(
    stock_labels_list,
    start_date,
    end_date,
    load_folder_name=DEFAULT_STOCK_DOWNLOAD_FOLDER_NAME,
    root_folder_path=None, 
    stock_data_prefix="stock_data"
):
    if root_folder_path is None:
        root_folder_path = get_current_directory()
    
    load_folder_path = path.join(
        root_folder_path,
        load_folder_name,
        "{date_from}_to_{date_to}".format(
            date_from=str(start_date).replace("-",""),
            date_to=str(end_date).replace("-","")
        )
    )
    
    existing_stockfile_data = [
        (file.split(".")[0].split("_")[-1], path.join(load_folder_path, file)) 
        for file in listdir(load_folder_path) 
        if path.isfile(path.join(load_folder_path, file)) and stock_data_prefix in file
    ]
    
    stocks_dictionary = {
        stock_name: pd.read_parquet(file_path) for stock_name, file_path in existing_stockfile_data
    }
    
    return stocks_dictionary