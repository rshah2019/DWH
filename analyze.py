import pandas as pd
import scipy.stats
from scipy.stats import norm
import numpy as np


def annual_vol(returns):
    """
    """
    returns = returns / 100
    annualized_vol = returns.std() * np.sqrt(12)
    return annualized_vol


def annual_return(returns):
    returns = returns / 100
    n_months = returns.shape[0]
    return_per_month = (returns + 1).prod() ** (1 / n_months) - 1
    annualized_return = (return_per_month + 1) ** 12 - 1
    return annualized_return


def cvar_historic(r, level=5):
    if isinstance(r, pd.Series):
        is_beyound = r <= -var_historic(r, level=level)
        return -r[is_beyound].mean()
    elif isinstance(r, pd.DataFrame):
        return r.aggregate(cvar_historic, level=level)
    else:
        raise TypeError("r should be Series or DataFrame")


def var_gaussian(r, level=5, modified=False):
    z = norm.ppf(level / 100)
    if modified:
        s = skewness(r)
        k = kurtosis(r)
        z = (z +
             (z ** 2 - 1) * s / 6 +
             (z ** 3 - 3 * z) * (k - 3) / 24 -
             (2 * z ** 3 - 5 * z) * (s ** 2) / 36
             )
    return -(r.mean() + z * r.std(ddof=0))


def var_historic(r, level=5):
    if isinstance(r, pd.DataFrame):
        return r.aggregate(var_historic, level=level)
    elif isinstance(r, pd.Series):
        return -np.nanpercentile(r, level)
    else:
        raise TypeError("Expected r to be Series or DataFrame")


def semideviations(r):
    is_negative = r < 0
    return r[is_negative].std(ddof=0)






def drawdown(return_series: pd.Series):
    wealth_index = 1000 * (1 + return_series).cumprod()
    previous_peaks = wealth_index.cummax()
    drawdowns = (wealth_index - previous_peaks) / previous_peaks
    return pd.DataFrame({
        "Wealth": wealth_index,
        "Peaks": previous_peaks,
        "Drawdown": drawdowns
    })


def skewness(r):
    """
    Alternatives to scipy.states.skew()
    computes skewness of supplied series
    returns a float or a series
    """
    demenaed_r = r - r.mean()
    # use the population std deviation so set dof = 0
    sigma_r = r.std(ddof=0)
    exp = (demenaed_r ** 3).mean()
    return exp / sigma_r ** 3


def kurtosis(r):
    """
    Alternatives to scipy.states.kurtosis()
    computes kurtosis of supplied series
    returns a float or a series
    """
    demenaed_r = r - r.mean()
    # use the population std deviation so set dof = 0
    sigma_r = r.std(ddof=0)
    exp = (demenaed_r ** 4).mean()
    return exp / sigma_r ** 4


def is_normal(r, level=0.01):
    statistis, p_value = scipy.stats.jarque_bera(r)
    return p_value > level