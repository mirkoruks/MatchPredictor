import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

def prepare_data(df, keep):
    
    keep_valid = {"mean5","mean10","mean15"}
    
    if len([keep]) != 1:
        raise ValueError(f"keep must be one single element")
    
    if keep not in keep_valid:
        raise ValueError(f"drop must be one of {keep_valid}")
    keep_valid.remove(keep)
    print(f"Dropping columns ending with suffixes: {", ".join(keep_valid)}")
    #keep_valid = "\("+"|".join(keep_valid)+"\)"
    print(keep_valid)
    drop_cols = df.columns.str.endswith(tuple(keep_valid))
    df_final = df.loc[:, ~drop_cols].copy()
    df_final = df_final.drop(columns="match_id")
    
    df_final["date"] = pd.to_datetime(df_final["date"])
    df_final = df_final.replace([np.inf, -np.inf], np.nan).dropna().sort_values(by="date", ignore_index=True)
    df_final = df_final.drop(columns="date")
    #match_id = df_final["match_id"]
    
    corr_matrix = df_final.drop(columns = ["home_result"]).corr().abs()
    # Select upper triangle of correlation matrix
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
    # Find features with correlation greater than 0.95
    to_drop = [column for column in upper.columns if any(upper[column] > 0.95)]
    # Drop features 
    df_final.drop(to_drop, axis=1, inplace=True)

    # Create first split (Training vs. Test)
    df_train, df_test = train_test_split(df_final, shuffle=False, test_size=0.18)
    
    X_train = df_train.drop(columns="home_result")
    y_train = df_train["home_result"]
    
    X_test = df_test.drop(columns="home_result")
    y_test = df_test["home_result"]
    
    return X_train, y_train, X_test, y_test


