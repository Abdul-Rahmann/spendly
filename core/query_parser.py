import pandas as pd
from datetime import datetime, timedelta

class TransactionQueryParser:
    def __init__(self, df):
        self.df = df
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['deposits'] = self.df['deposits'].astype(float)
        self.df['withdrawals'] = self.df['withdrawals'].astype(float)
        print('Transaction Dataframe loaded successfully!')

    def filter_by_date(self, start_date=None, end_date=None):
        df_filtered = self.df

        if start_date:
            df_filtered = df_filtered[df_filtered['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df_filtered = df_filtered[df_filtered['date'] <= pd.to_datetime(end_date)]
        print(f"Filtered {len(df_filtered)} rows between {start_date} and {end_date}.")
        return df_filtered

    def filter_by_description(self, keyword):
        df_filtered = self.df[self.df['description'].str.contains(keyword, case=False, na=False)]
        print(f"Found {len(df_filtered)} rows matching keyword: '{keyword}'.")
        return df_filtered

    def get_total_amount(self, transaction_type='withdrawals', start_date=None, end_date=None):
        valid_transaction_types = ['withdrawals', 'deposits']
        if transaction_type not in valid_transaction_types:
            raise ValueError(f"Invalid transaction type. Choose from: {valid_transaction_types}")

        df_filtered = self.filter_by_date(start_date, end_date)
        total = df_filtered[transaction_type].sum()
        print(f"Total {transaction_type} between {start_date} and {end_date}: ${total:.2f}")
        return total

    def get_largest_transaction(self, transaction_type='withdrawals'):
        valid_transaction_types = ['withdrawals', 'deposits']

        if transaction_type not in valid_transaction_types:
            return ValueError(f"Invalid transaction type. Choose from: {valid_transaction_types}")

        largest_transaction = self.df[transaction_type].idxmax()
        print(f"Largest {transaction_type}: ${largest_transaction:,.2f}")
        return largest_transaction

    def query(self, query_type, **kwargs):
        if query_type == 'date':
            return self.filter_by_date(**kwargs)
        elif query_type == 'description':
            return self.filter_by_description(**kwargs)
        elif query_type == 'total':
            return self.get_total_amount(**kwargs)
        elif query_type == 'largest':
            return self.get_largest_transaction(**kwargs)
        else:
            raise ValueError(f"Unknown query type: {query_type}")

if __name__ == "__main__":
    transactions = pd.read_csv('../data/processed/transactions.csv')
    query_parser = TransactionQueryParser(transactions)

    # Sample queries
    print("\n--- Total Withdrawals Last Month ---")
    last_month = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    query_parser.get_total_amount(transaction_type='withdrawals', start_date=last_month)

    print("\n--- Transactions in August 2023 ---")
    august_transactions = query_parser.filter_by_date(start_date='2024-08-01', end_date='2024-08-31')
    print(august_transactions)

    print("\n--- Largest Deposit ---")
    largest_deposit = query_parser.get_largest_transaction(transaction_type='deposits')
    print(largest_deposit)

    print("\n--- Groceries Transactions ---")
    groceries = query_parser.filter_by_description(keyword='groceries')
    print(groceries)

