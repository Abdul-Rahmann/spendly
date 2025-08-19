import pdfplumber
import pandas as pd
import re
import wordninja
import os
import numpy as np


class DataProcessor:
    """
    Handles data extraction and processing from PDF and CSV files.
    """

    DEPOSIT_TRIGS = ['Deposit', 'MB-Transferfrom']

    @staticmethod
    def split_concatenated_text(text):
        return " ".join(wordninja.split(text))

    @staticmethod
    def extract_year(lines):
        """
        Extracts the year from the lines of text in a PDF
        Returns the first valid 4-digit year found or None if no year is detected.
        """

        for line in lines:
            year_match = re.search(r'\b(20\d{2})\b', line)
            if year_match:
                return year_match.group(1)

        return None

    def extract_transactions(self, lines, extracted_year):
        """
        Extracts structured transactions from lines if text on a page.
        Returns a list of transactions
        """

        transactions = []
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
                  "Aug", "Sep", "Oct", "Nov", "Dec"]

        for line in lines:
            line = line.strip()
            if any(line.startswith(month) for month in months):
                parts = line.split()
                if not parts[-1].isdigit() and parts[-1].isalpha():
                    del parts[-1]

                # Extract date and add year if available
                date = parts[0]
                if extracted_year:
                    date = f"{date}{extracted_year}"
                    try:
                        date = pd.to_datetime(date, format='%b%d%Y').strftime('%Y-%m-%d')
                    except ValueError:
                        continue

                # Extract balance
                balance = parts[-1]

                # Determine whether transaction is deposit or withdrawal
                if any(tag in parts for tag in self.DEPOSIT_TRIGS):
                    deposit = parts[-2]
                    withdrawal = None
                else:
                    deposit = None
                    withdrawal = parts[-2]

                # Extract and clean description
                description = ' '.join(
                    [part for part in parts[1:-2] if not re.match(r'^\d+(\.\d{1,2})?$', part)]
                )
                description = self.split_concatenated_text(description)

                # Append transaction
                transactions.append([date, description, withdrawal, deposit, balance])
            elif transactions:
                # Handle multi-line descriptions
                transactions[-1][1] += ' ' + self.split_concatenated_text(line)

        return transactions


    def process_pdf(self, file_path):
        """
        Extracts transactions from a single PDF file.
        Returns a pandas DataFrame.
        """
        transactions = []

        with pdfplumber.open(file_path) as pdf:
            extracted_year = None

            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split('\n')

                if not extracted_year:
                    extracted_year = self.extract_year(lines)

                transactions.extend(self.extract_transactions(lines, extracted_year))

        columns = ['Date', 'Description', 'Withdrawals ($)', 'Deposits ($)', 'Balance ($)']
        return self.preprocess_transactions(pd.DataFrame(transactions, columns=columns))

    # Quick validation when running data_processor.py independently

    def preprocess_transactions(self, df):
        """
        Preprocesses the raw transactions DataFrame:
        - Renames columns to lowercase.
        - Handles and removes 'Opening Balance' and 'Closing Balance' rows.
        - Converts withdrawals, deposits, and balance to numeric format.
        - Adds derived columns like transaction type, day, month, etc.
        Returns the cleaned DataFrame.
        """
        # Column renaming
        renames = {
            'Date': 'date',
            'Description': 'description',
            'Withdrawals ($)': 'withdrawals',
            'Deposits ($)': 'deposits',
            'Balance ($)': 'balance'
        }
        df = df.rename(columns=renames)

        # Handle 'Opening Balance' and 'Closing Balance'
        df.loc[df['withdrawals'] == 'OpeningBalance', 'description'] = 'Opening Balance'
        df.loc[df['withdrawals'] == 'OpeningBalance', 'withdrawals'] = np.nan
        df.loc[df['withdrawals'] == 'ClosingBalance', 'description'] = 'Closing Balance'
        df.loc[df['withdrawals'] == 'ClosingBalance', 'withdrawals'] = np.nan

        # Convert numeric columns
        for col in ['withdrawals', 'deposits', 'balance']:
            df[col] = df[col].str.replace(',', '').str.replace('$', '').astype(np.float32)

        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Drop rows with invalid dates or balances
        df = df.dropna(subset=['date', 'balance'])

        # Drop 'Opening Balance' or 'Closing Balance' rows
        df = df[df['description'] != 'Opening Balance']
        df = df[df['description'] != 'Closing Balance']

        # Add transaction type
        df['transaction_type'] = df.apply(
            lambda row: 'withdrawal' if pd.notna(row['withdrawals']) and row['withdrawals'] > 0
            else 'deposit' if pd.notna(row['deposits']) and row['deposits'] > 0
            else 'unknown',
            axis=1
        )

        # Add date-related derived columns
        df['day'] = df['date'].dt.day
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year
        df['day_of_week'] = df['date'].dt.dayofweek


        def categorize_transaction(description):
            description = description.lower()
            if any(keyword in description for keyword in
                   ["chitchat super", "no frills", "walmart", "dollarama", "african super"]):
                return "Groceries"
            elif any(keyword in description for keyword in
                     ["subway", "tim hortons", "cineplex", "granville island", "ocean garden", "shipyards chris"]):
                return "Restaurants/Dining"
            elif any(keyword in description for keyword in ["golds gym", "ubc gym","gym"]):
                return "Fitness"
            elif any(keyword in description for keyword in
                     ["canadian tire", "amazon", "london drugs", "footlocker", "kindle","temu"]):
                return "Shopping"
            elif any(keyword in description for keyword in
                     ["cineplex", "compass web", "netflix", "amazon prime"]):
                return "Entertainment"
            elif any(keyword in description for keyword in
                     ["driver services", "tennis vancouver", "compass web", "revenue services"]):
                return "Transportation"
            elif any(keyword in description for keyword in
                     ["tuition", "ubc botanical", "campus vision", "education ref", "ubc enrolment"]):
                return "Education"
            elif "error correction" in description:
                return "Error Corrections"
            elif any(keyword in description for keyword in ["virgin plus", "heroku charge", "kindle"]):
                return "Light Subscriptions"
            elif any(keyword in description for keyword in ["overdrawn handling", "overdraft interest"]):
                return "Overdraft/Fees"
            elif "refund" in description:
                return "Refunds"
            elif "mb transfer" in description:
                return "Transfers"
            elif description.isnumeric() and float(description) > 1000:  # Example large purchase threshold
                return "Large Purchases"
            else:
                return "Other"

        df['category'] = df['description'].apply(categorize_transaction)

        df['is_recurring'] = df.groupby(['description','month','year'])['description'].transform('count') > 1

        # Add alerts for balance thresholds
        df['low_balance_alert'] = df['balance'] < 100
        df['critical_balance_alert'] = df['balance'] < 20
        df['overdraft_alert'] = df['balance'] < 0

        # Add flags for large transactions
        df['is_large_withdrawal'] = df['withdrawals'] > 10000
        df['is_large_deposit'] = df['deposits'] > 10000

        df['month_withdrawals'] = df.groupby(['month','year'])['withdrawals'].transform('sum')
        df['monthly_deposits'] = df.groupby(['month','year'])['deposits'].transform('sum')
        df['monthly_start_balance'] = df.groupby(['month','year'])['balance'].transform('first')
        df['monthly_end_balance'] = df.groupby(['month','year'])['balance'].transform('last')
        df['monthly_balance_change'] = df['monthly_end_balance'] - df['monthly_start_balance']

        df['seasonal_spending'] = df['month'].isin([11,12]) | df['month'].isin([8,9])

        # Add refund and error correction flags
        df['is_refund'] = df['description'].str.contains('refund', case=False, na=False)
        df['is_error_correction'] = df['description'].str.contains('error correction', case=False, na=False)

        print(f"Preprocessed DataFrame – {len(df)} valid transactions with enriched features.")

        return df

    def process_directory(self, directory_path):
        """
        Processes all PDF files in a directory and consolidates transactions.
        Returns a single pandas DataFrame with all transactions.
        """
        all_transactions = pd.DataFrame()
        file_count = 0

        for file_name in os.listdir(directory_path):
            if file_name.endswith('.pdf'):
                file_path = os.path.join(directory_path, file_name)
                try:
                    transactions = self.process_pdf(file_path)
                    all_transactions = pd.concat([all_transactions, transactions], ignore_index=True)
                    file_count += 1
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

        if file_count == 0:
            print(f"No PDF files found in directory: {directory_path}")
        else:
            print(f"Processed {file_count} files from {directory_path}.")

        return all_transactions


# Run as standalone script for validation
if __name__ == "__main__":
    processor = DataProcessor()
    input_directory = "../data/raw"
    output_csv = "../data/processed/transactions.csv"

    if os.path.exists(input_directory):
        print(f"Processing PDFs from directory: {input_directory}")
        transactions_df = processor.process_directory(input_directory)

        if not transactions_df.empty:
            # Save to CSV
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            transactions_df.to_csv(output_csv, index=False)
            print(f"Consolidated transactions saved to {output_csv}.")
        else:
            print("No transactions extracted.")
    else:
        print(f"Input directory not found: {input_directory}")






