import pandas as pd

# A function is defined to compute the RSI function of talib package
def calc_rsi(series, timeperiod):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(timeperiod, min_periods=timeperiod).mean()
    avg_loss = loss.rolling(timeperiod, min_periods=timeperiod).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi

def get_momentum_rank(df, pct_window = 3, std_window = 7, rsi_window = 20, df_vol = None):

    # Compute the 1-day percent change using the pct_change function
    df_pct = df.pct_change()

    # Add the past two days returns using the rolling_sum function
    df_pct_roll = df_pct.rolling(pct_window).sum()

    # Daily rank the cryptos in ascending order
    pct_ranks = df_pct_roll.rank(axis=1, ascending=True)

    # Compute the rolling standard deviation for the past 7 days
    df_std_roll = df_pct.rolling(std_window).std()

    # Daily rank the cryptos in descending order
    std_ranks = df_std_roll.rank(axis=1, ascending=False)

    # Compute the RSI
    df_rsi = df.apply(lambda r: calc_rsi(r, rsi_window), axis=0)

    # Daily rank the RSI values in ascending order
    rsi_ranks = df_rsi.rank(axis=1, ascending=True)

    if df_vol is not None:
        df_vol_ratio = df_vol.rolling(3).mean() / df_vol.rolling(20).mean()
        df_vol_rank = df_vol_ratio.rank(axis = 1, ascending = True)        
        combined_score = pct_ranks + std_ranks + rsi_ranks + df_vol_rank
    else:
        combined_score = pct_ranks + std_ranks + rsi_ranks
        
    # Rank the combined score
    combined_ranks = combined_score.rank(axis=1)

    # Print last five rows of the combined_ranks
    combined_ranks = combined_ranks.dropna()

    return combined_ranks, df_pct_roll, pct_ranks, std_ranks, df_rsi, rsi_ranks 

# combined_ranks, df_pct_3, pct_3_ranks, std_7_ranks, df_rsi_14, rsi_14_ranks = get_momentum_rank(df, df_vol = df_vol)
# # Compute the 1-day percent change using the pct_change function

def calculate_momentum(df_price, pct_window=3, std_window=6, rsi_window=20, in_week = False):
    """
    Calculate momentum ranks based on price and volume data.
    
    Parameters:
    - df_price: DataFrame containing price data.
    - pct_window: Window size for percent change calculation.
    - std_window: Window size for standard deviation calculation.
    - rsi_window: Window size for RSI calculation.
    
    Returns:
    - combined_ranks: DataFrame of combined momentum ranks.
    - df_pct_3: DataFrame of 3-day percent changes.
    - pct_3_ranks: DataFrame of percent change ranks.
    - std_7_ranks: DataFrame of 7-day standard deviation ranks.
    - df_rsi_14: DataFrame of RSI values.
    - rsi_14_ranks: DataFrame of RSI ranks.
    """
    df_pct = df_price.pct_change()
    stock_list = df_price.columns.tolist()
    combined_ranks, df_pct_3, pct_3_ranks, std_7_ranks, df_rsi_14, rsi_14_ranks = get_momentum_rank(df_price, pct_window = pct_window, std_window = std_window, rsi_window = rsi_window)

    num_stocks = 8
    # Generate trading signal
    def signal_generator(rank, l = 20, num_stocks = 8):
        # If the rank is less than 6 than set the signal to 0 to indicate no positions

        rank[rank <= l - num_stocks] = 0
        rank[rank > l - num_stocks] = 1
        # return the rank dataframe
        return rank

    # Call the signal_generator to decide on cryptocurrency to go long on
    signal = combined_ranks.copy().apply(lambda r: signal_generator(r, len(stock_list), num_stocks), axis=0)
    signal = signal.dropna()

    if in_week:
        print("Live trading mode activated. Displaying current signals.")        
        today = signal.iloc[-2]
    else:
        today = signal.iloc[-1]

    stock_list = []
    for i in range(len(signal.columns)):
        if today[i] == 1:
            stock_list.append(signal.columns[i])

    return stock_list


