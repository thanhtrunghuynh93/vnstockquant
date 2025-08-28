import numpy as np
import matplotlib.pyplot as plt

# Plot the portfolio returns
def plot_returns_dd(portfolio, res_col = "returns", fee = 0.001):
    # ----------- Sharpe ratio ------------------
    sharpe_ratio = np.mean(portfolio[res_col])/np.std(portfolio[res_col])*(252**0.5)
    print('The Sharpe ratio is %.2f ' % sharpe_ratio)

    # ----------- Cumulative strategy returns ------------------
    portfolio['cum_str_returns'] = (portfolio[res_col] + 1 - fee).cumprod()
    # Plot the cumulative strategy returns
    portfolio['cum_str_returns'].plot(figsize=(10,7), color='green')
    plt.title('Strategy Returns', fontsize=14)
    plt.ylabel('Cumulative returns')
    plt.show()        

    # ----------- Drawdown ------------------    
    # Calculate the running maximum
    running_max = np.maximum.accumulate(portfolio['cum_str_returns'].dropna())
    # Ensure the value never drops below 1
    running_max[running_max < 1] = 1
    # Calculate the percentage drawdown
    drawdown = (portfolio['cum_str_returns'])/running_max - 1
    max_dd = drawdown.min()*100
    print('The maximum drawdown is %.2f' % max_dd)
    # Plot the drawdowns
    drawdown.plot(color='r',figsize=(10,10))
    plt.ylabel('Returns')
    plt.fill_between(drawdown.index, drawdown, color='red')
    plt.grid(which="major", color='k', linestyle='-.', linewidth=0.2)
    plt.show()    
    
# Build function for essential strategy metrics
def analysis(p, mode = "1d"):

    # Calculate number of years
    if mode == "1d":
        n = len(p)/252
        scale = 252
    elif mode == "1h":
        n = len(p)/252/5
        scale = 252 * 5
    else:
        assert("Time mode not supported")

    # Calculate CAGR
    CAGR = ((1+p).prod()**(1/n)-1)

    # Calculate Sharpe Ratio
    Sharpe = p.mean()/p.std()*np.sqrt(scale)

    # Calculate Sortino Ratio
    Sortino = p.mean()/p[p < 0].std()*16

    # Calculate Profit Factor
    PF = np.inf if p[p < 0].sum() == 0 else -p[p > 0].sum()/p[p < 0].sum()

    # Calculate Maximum Drawdown
    cum_return = (1+p).cumprod().dropna()
    # Calculate Maximum Drawdown    
    running_max = np.maximum.accumulate(cum_return)
    # Ensure the value never drops below 1
    running_max[running_max < 1] = 1
    # Calculate the percentage drawdown
    drawdown = cum_return/running_max - 1
    max_ddwn = drawdown.min()
    # max_ddwn = (p.add(1).cumprod()-p.add(1).cumprod().expanding().max()).min()

    print(f'CAGR: {CAGR:.2f}, Sharpe: {Sharpe:.2f}, Sortino: {Sortino:.2f}, MaxDDWN: {max_ddwn:.2f} PF: {PF:.2f}')
    return CAGR, Sharpe, Sortino, PF, max_ddwn
# plot_returns_dd(portfolio)    