import pdfplumber
import pandas as pd
import re
import wordninja
import os

DEPOSIT_TRIGS = ['Deposit', 'MB-Transferfrom']

def split_concatenated_text(text):
    """Splits concatenated text into readable words using wordninja."""
    return " ".join(wordninja.split(text))

def extract_year(lines):
    """
    Extracts the year from lines of text in a PDF.
    Returns the first valid 4-digit year found or None if no year is detected.
    """
    for line in lines:
        year_match = re.search(r'\b(20\d{2})\b', line)
        if year_match:
            return year_match.group(1)
    return None

def extract_transactions_from_page(lines, extracted_year):
    """
    Extracts transaction records from lines of text on a single page.
    Returns a list of structured transactions.
    """
    transactions = []
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    for line in lines:
        line = line.strip()
        if any(line.startswith(month) for month in months):
            parts = line.split()
            if not parts[-1].isdigit() and parts[-1].isalpha():
                del parts[-1]

            date = parts[0]
            if extracted_year:
                date = f"{date}{extracted_year}"
                try:
                    date = pd.to_datetime(date, format='%b%d%Y').strftime('%b %d, %Y')
                except ValueError:
                    continue

            balance = parts[-1]
            if any(tag in parts for tag in DEPOSIT_TRIGS):
                deposit = parts[-2]
                withdrawal = None
            else:
                deposit = None
                withdrawal = parts[-2]

            description = ' '.join([part for part in parts[1:-2] if not re.match(r'^\d+(\.\d{1,2})?$', part)])
            description = split_concatenated_text(description)
            transactions.append([date, description, withdrawal, deposit, balance])
        elif transactions:
            transactions[-1][1] += ' ' + split_concatenated_text(line)

    return transactions

def process_single_pdf(file_path):
    """
    Processes a single PDF file and extracts all transactions from it.
    Returns a DataFrame of all transactions in the file.
    """
    transactions = []

    with pdfplumber.open(file_path) as pdf:
        extracted_year = None

        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')

            if not extracted_year:
                extracted_year = extract_year(lines)

            transactions.extend(extract_transactions_from_page(lines, extracted_year))

    columns = ['Date', 'Description', 'Withdrawals ($)', 'Deposits ($)', 'Balance ($)']
    return pd.DataFrame(transactions, columns=columns)

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
            transactions = process_single_pdf(file_path)
            all_transactions = pd.concat([all_transactions, transactions], ignore_index=True)

    return all_transactions

if __name__ == "__main__":
    # Directory containing the PDF files
    directory_path = "data/statements"
    consolidated_transactions_df = process_all_pdfs_in_directory(directory_path)
consolidated_transactions_df