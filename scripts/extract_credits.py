import pdfplumber
import pandas as pd
import re
import wordninja
import os


# def extract_year(text):
#     # Simplified logic: Find the year in date formats with spaces before months
#     match = re.search(r"\s[A-Za-z]{3}\s\d{1,2},\s(\d{4})", text)
#     if match:
#         return int(match.group(1))
#     return None


def extract_transactions_from_pdf(pdf_file):
    """
    Extract transactions from a financial statement PDF based on the pattern:
    Date (with year), Description, Withdrawn ($), Deposited ($), Balance ($).
    """
    transactions = []
    extracted_year = None  # Variable to store the extracted year

    with pdfplumber.open(pdf_file) as pdf:
        data = []

        statement_year = None  # Variable to hold the statement year
        for page_number, page in enumerate(pdf.pages, start=1):
            text = page.extract_text()
            print(f"Processing page {page_number}...")

            # Find the year if not already found
            if statement_year is None:
                year_match = re.search(r"\s[A-Za-z]{3}\s\d{1,2},\s(\d{4})", text)
                if year_match:
                    statement_year = int(year_match.group(1))
                    print(f"Extracted Year: {statement_year}")
                else:
                    print("Year not found on this page!")

            # Extract transactions based on your pattern (ensure the year is used)
            transaction_pattern = r"(\d{3})\s([A-Za-z]{3}\s\d{1,2})\s([A-Za-z]{3}\s\d{1,2})\s([\w\s\*\-\/\.,]+?)\s([\d,]+\.\d{2}(?:-|\d+)?)"
            transactions += re.findall(transaction_pattern, text)

        # Process transactions found
        for transaction in transactions:
            reference, trans_date, post_date, description, amount = transaction
            # Only append the year if it has been found
            if statement_year is not None:
                trans_date = f"{trans_date} {statement_year}"
            amount = float(amount.replace(',', '').replace('-', ''))

            data.append({
                'date': trans_date,
                'description': description.strip(),  # Clean up whitespace
                'amount': amount,
            })

        return pd.DataFrame(data)


def process_all_pdfs_in_directory(directory_path):
    """
    Reads all PDF files in a specified directory and extracts transactions.
    Returns a single consolidated DataFrame of all transactions.
    """
    all_transactions = pd.DataFrame()

    for file_name in os.listdir(directory_path):
        if file_name.endswith('.pdf'):
            file_path = os.path.join(directory_path, file_name)
            print(f"Processing file: {file_name}")
            transactions = extract_transactions_from_pdf(file_path)
            # print(transactions)
            all_transactions = pd.concat([all_transactions, transactions], ignore_index=True)

    return all_transactions



# Example Usage
if __name__ == "__main__":
    # File path of the statement PDF
    path = "../data/raw/cheq/"
    # Extract transactions into a DataFrame
    transactions_df = process_all_pdfs_in_directory(path)
    transactions_df.to_csv("../data/processed/credit-transactions.csv", index=False)
transactions_df