import sqlite3
import pandas as pd
import numpy as np

def init_messy_db():
    conn = sqlite3.connect("finance.db")
    
    # --- TABLE 1: Global_Accounts (The "Gold" Source) ---
    accounts_df = pd.DataFrame({
        'ACCT_ID': [101, 102, 103, 104],
        'TP_CODE': ['CHK', 'SAV', 'INV', 'INV'], # Abbreviations are harder for LLMs
        'BAL_AMT': [25000.50, 150000.00, 500000.00, -99999.00], # -99999 is a "dirty" outlier
        'CURRENCY': ['USD', 'usd', 'U.S. Dollar', 'USD'], # Inconsistent casing/naming
        'LAST_UPDATED': ['2026-01-01', '01/05/26', 'March 10, 2026', None] # Messy date formats
    })
    
    # --- TABLE 2: Transactions_v2_Final_Actual (Realistic messy naming) ---
    # Trap: This table uses 'ACC_REF' instead of 'ACCT_ID'
    tx_df = pd.DataFrame({
        'TX_REF_ID': range(1, 6),
        'ACC_REF': [101, 101, 102, 105, 103], # 105 is an 'Orphan' record (doesn't exist in accounts)
        'VAL': [1200.0, "5000.0", -500.0, 1500.0, 0.0], # Mixed types: some floats, some strings
        'DESC_FLD': ['RENT_PAY', 'SALARY', 'UTILITY', 'DIVIDEND', 'ERROR_LOG'],
        'TS': pd.to_datetime(['2026-03-01', '2026-03-05', '2026-03-02', '2026-03-10', '2026-03-11'])
    })

    # --- TABLE 3: Internal_Metadata (Vague column names) ---
    # Trap: 'col_a' and 'col_b' mean nothing without context
    meta_df = pd.DataFrame({
        'col_a': [101, 102, 103],
        'col_b': ['High Priority', 'Standard', 'VIP Client'],
        'notes': ['Legacy system migration', None, 'Verify manually']
    })

    accounts_df.to_sql('accounts', conn, if_exists='replace', index=False)
    tx_df.to_sql('transactions_legacy', conn, if_exists='replace', index=False)
    meta_df.to_sql('account_metadata', conn, if_exists='replace', index=False)
    
    print("🚀 'Legacy Swamp' Database created!")
    conn.close()

if __name__ == "__main__":
    init_messy_db()