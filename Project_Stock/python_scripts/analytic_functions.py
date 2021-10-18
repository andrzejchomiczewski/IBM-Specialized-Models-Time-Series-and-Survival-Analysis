import pandas as pd
import statsmodels.tsa.stattools as ts
from matplotlib import pyplot as plt


def get_rolling_means_dataframe(
    stocks_dictionary,
    value_column = "High",
    rolling_mean_interval = "7D"
):
    stocks_rolling_means_list = [
        pd.DataFrame(
            values_dataframe[value_column].rolling(rolling_mean_interval).mean()
        ).rename(columns = {value_column:stock+"_"+rolling_mean_interval+"_mean"})
        for stock, values_dataframe in stocks_dictionary.items()
    ]
    
    stocks_rolling_means = pd.concat(stocks_rolling_means_list, axis=1, ignore_index=False)
    
    return stocks_rolling_means

def get_rolling_stds_dataframe(
    stocks_dictionary,
    value_column = "High",
    rolling_mean_interval = "7D"
):
    stocks_rolling_std_list = [
        pd.DataFrame(
            values_dataframe[value_column].rolling(rolling_mean_interval).std()
        ).rename(columns = {value_column:stock+"_"+rolling_mean_interval+"_std"})
        for stock, values_dataframe in stocks_dictionary.items()
    ]
    
    stocks_rolling_std = pd.concat(stocks_rolling_std_list, axis=1, ignore_index=False)
    
    return stocks_rolling_std

def get_expanding_means_dataframe(
    stocks_dictionary,
    value_column = "High"
):
    stocks_expanding_list = [
        pd.DataFrame(
            values_dataframe[value_column].expanding().mean()
        ).rename(columns = {value_column:stock+"_expanding_mean"})
        for stock, values_dataframe in stocks_dictionary.items()
    ]
    
    stocks_expanding_means = pd.concat(stocks_expanding_list, axis=1, ignore_index=False)
    
    return stocks_expanding_means

def get_expanding_stds_dataframe(
    stocks_dictionary,
    value_column = "High",
):
    stocks_expanding_std_list = [
        pd.DataFrame(
            values_dataframe[value_column].expanding().std()
        ).rename(columns = {value_column:stock+"_expanding_std"})
        for stock, values_dataframe in stocks_dictionary.items()
    ]
    
    stocks_expanding_std= pd.concat(stocks_expanding_std_list, axis=1, ignore_index=False)
    
    return stocks_expanding_std

def determine_normalized_timeseries_data(
    timeseries_data_dictionary,
    timeseries_column = "High_pct_change"
):
    normalized_timeseries_data_dictionary = {}
    for timeseries in timeseries_data_dictionary:
        timeseries_data = timeseries_data_dictionary.get(timeseries)
        timeseries_data = timeseries_data[~timeseries_data[timeseries_column].isna()].copy(deep = True)
        timeseries_mean = timeseries_data[timeseries_column].mean()
        timeseries_std = timeseries_data[timeseries_column].std()
        timeseries_data["normalized_timeseries"] = (timeseries_data[timeseries_column] - timeseries_mean)/timeseries_std
        timeseries_data_dictionary[timeseries] = timeseries_data["normalized_timeseries"]
    return timeseries_data_dictionary

def dickery_fuller_test(timeseries):
    dftest = ts.adfuller(timeseries,)
    dfoutput = pd.Series(dftest[0:4], 
                         index=['Test Statistic','p-value','Lags Used','Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print(dfoutput)
    #Determing rolling statistics
    rolmean = timeseries.rolling(window=12).mean()
    rolstd = timeseries.rolling(window=12).std()

    #Plot rolling statistics:
    orig = plt.plot(timeseries, color='blue',label='Original')
    mean = plt.plot(rolmean, color='red', label='Rolling Mean')
    std = plt.plot(rolstd, color='black', label = 'Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean and Standard Deviation')
    plt.grid()
    plt.show(block=False)