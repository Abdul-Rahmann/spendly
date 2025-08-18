import pandas as pd
from datetime import datetime, timedelta
import re

class TransactionQueryParser:
    def __init__(self, df):
        self.df = df
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df['deposits'] = self.df['deposits'].astype(float)
        self.df['withdrawals'] = self.df['withdrawals'].astype(float)
        print('Transaction Dataframe loaded successfully!')

    @staticmethod
    def parse_relative_dates(relative_date):
        today = datetime.now()

        if re.match('last week', relative_date, re.IGNORECASE):
            start_date = today - timedelta(days=7)
            end_date =  start_date + timedelta(days=6)
        elif re.match('this month', relative_date, re.IGNORECASE):
            start_date = today - timedelta(days=today.weekday())
            end_date = today
        elif re.match('last month', relative_date, re.IGNORECASE):
            first_day_this_month = today.replace(day=1)
            end_date = first_day_this_month - timedelta(days=1)
            start_date = end_date - timedelta(days=30)
        elif re.match('this month', relative_date, re.IGNORECASE):
            start_date = today.replace(day=1)
            end_date = today
        elif re.match('last year', relative_date, re.IGNORECASE):
            start_date = today.replace(month=1, day=1)
            end_date = today
        else:
            raise ValueError(f"Invalid relative date: {relative_date}")

        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    def filter_by_date(self, start_date=None, end_date=None):

        if start_date and isinstance(start_date, str) and " " in start_date:
            start_date, end_date = self.parse_relative_dates(start_date)

        df_filtered = self.df

        if start_date:
            df_filtered = df_filtered[df_filtered['date'] >= pd.to_datetime(start_date)]
        if end_date:
            df_filtered = df_filtered[df_filtered['date'] <= pd.to_datetime(end_date)]
        print(f"Filtered {len(df_filtered)} rows between {start_date} and {end_date}.")
        return df_filtered

    def filter_by_description(self, keywords):
        if isinstance(keywords, str):
            keywords = [keywords]

        pattern = '|'.join(re.escape(keyword) for keyword in keywords)
        df_filtered = self.df[self.df['description'].str.contains(pattern, case=False, na=False)]
        # df_filtered = self.df[self.df['description'].str.contains(keyword, case=False, na=False)]
        print(f"Found {len(df_filtered)} rows matching keywords: {keywords}.")
        return df_filtered

    def get_total_amount(self, transaction_type='withdrawals', start_date=None, end_date=None):
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
    groceries = query_parser.filter_by_description(keywords='groceries')
    print(groceries)

    # Sample Query Execution
    print("\n--- Total Withdrawals Last Month ---")
    query_parser.get_total_amount(transaction_type='withdrawals', start_date="last month")

    print("\n--- Groceries Transactions ---")
    groceries = query_parser.filter_by_description(keywords=["groceries", "walmart"])
    print(groceries)

    print("\n--- Largest Deposit ---")
    largest_deposit = query_parser.get_largest_transaction(transaction_type='deposits')
    print(largest_deposit)


