# Imports
import yfinance as yf
from pytickersymbols import PyTickerSymbols
import pandas as pd
from curl_cffi import requests

session = requests.Session(impersonate="chrome")
from tqdm import tqdm
import numpy as np
import os
import requests
import tempfile
import pdfplumber
import re
from typing import List
import tabula


# Definition of core functions
def get_oprev_growth(ticker: str) -> float | None:
    """
    Calculate the growth in operating revenue over the past four reporting periods for a given stock ticker.

    Parameters:
        ticker (str): The stock ticker symbol (e.g., 'AAPL').

    Returns:
        float | None: The ratio of the most recent operating revenue to the operating revenue from four periods ago.
                      Returns None if data is missing or insufficient. Returns 'Nan' as a string if the income statement
                      is empty or has fewer than four columns.
    """
    stock = yf.Ticker(ticker)
    incomestmt = stock.income_stmt

    if incomestmt.empty:
        return "Nan"

    if incomestmt.shape[1] < 4:
        return "Nan"  # Not enough data to calculate growth

    latest_col = incomestmt.columns[0]
    earliest_col = incomestmt.columns[3]

    latestrev = incomestmt.loc["Operating Revenue"].get(latest_col, None)
    earliestrev = incomestmt.loc["Operating Revenue"].get(earliest_col, None)

    if (latestrev is None) or (earliestrev is None) or earliestrev == 0:
        return None

    return latestrev / earliestrev


def get_info(ticker: str) -> float | None:
    """
    Retrieves key financial metrics for a given stock ticker using the yfinance library.

    Parameters:
        ticker (str): The stock ticker symbol (e.g., 'AAPL' for Apple Inc.).

    Returns:
        dict: A dictionary containing the following financial metrics:
            - 'Sector': The sector the company operates in.
            - 'Debt/equity': The debt-to-equity ratio (converted to a percentage).
            - 'ROE': Return on equity.
            - 'PEG': PEG ratio (5 year expected).
            - 'Free Cash Flow': Free cash flow.
            - 'Revenue Growth': Revenue growth (from a custom function `get_oprev_growth`).
        If any metric is unavailable, its value will be 'Nan'.
    """
    stock = yf.Ticker(ticker)
    info = stock.info

    Industry = info.get("sector")
    if Industry is None:
        Industry = "Nan"
    DE = info.get("debtToEquity")
    if DE is None:
        DE = "Nan"
    ROE = info.get("returnOnEquity")
    if ROE is None:
        ROE = "Nan"
    PEG = info.get("trailingPegRatio")
    if PEG is None:
        PEG = "Nan"
    FCF = info.get("freeCashflow")
    if FCF is None:
        FCF = "Nan"
    REVG = get_oprev_growth(ticker)
    if REVG is None:
        REVG = "Nan"
    REC = info.get("recommendationMean")
    if REC is None:
        REC = "Nan"
    fPE = info.get("forwardPE")
    if fPE is None:
        fPE = "Nan"
    tPE = info.get("trailingPE")
    if tPE is None:
        tPE = "Nan"
    PB = info.get("priceToBook")
    if PB is None:
        PB = "Nan"
    SR = info.get("shortRatio")
    if SR is None:
        SR = "Nan"


    

    stock_info = {
        "Sector": Industry,
        "Debt/equity": float(DE) / 100,
        "ROE": ROE,
        "PEG": PEG,
        "Free Cash Flow": FCF,
        "Revenue Growth": REVG,
        "Recommendation": REC,
        'Forward PE': fPE,
        'Trailing PE': tPE,
        'Price/Book': PB,
        'Short Ratio': SR

    }

    return stock_info


def score_row(row, ind_av) -> int | None:
    """
    Calculate a score for a given row of financial data based on how it compares
    to industry averages.

    Parameters:
    row (pd.Series): A row from a DataFrame containing financial metrics for a company.
    ind_av (pd.DataFrame): A DataFrame containing industry average values for each metric,
                           indexed by industry sector.

    Returns:
    int: A score from 0 to 5 indicating how many metrics meet or exceed industry standards.
         One point is awarded for each of the following conditions:
         - Debt/equity is less than or equal to the industry average.
         - PEG is less than or equal to the industry average.
         - ROE is greater than or equal to the industry average.
         - Free Cash Flow is greater than or equal to the industry average.
         - Revenue Growth is greater than or equal to the industry average.
    """
    industry = row["Sector"]
    score = 0
    if (
        pd.notna(row["Debt/equity"])
        and row["Debt/equity"] <= ind_av.loc[industry, "Debt/equity"]
    ):
        score += 1
        if 0.5 < row["Debt/equity"] <= 1.0:
            score += 1
        elif 0.0< row['Debt/equity'] <= 0.5:
            score += 2


    if pd.notna(row["PEG"]) and row["PEG"] <= ind_av.loc[industry, "PEG"]:
        score += 1

        if 0.0 < row["PEG"] <= 1.0:
            score += 1
    
    if pd.notna(row["Forward PE"]) and row["Forward PE"] <= ind_av.loc[industry, "Forward PE"]:
        score += 1

    if pd.notna(row["Trailing PE"]) and row["Trailing PE"] <= ind_av.loc[industry, "Trailing PE"]:
        score += 1

    if pd.notna(row["Price/Book"]) and row["Price/Book"] <= ind_av.loc[industry, "Price/Book"]:
        score += 1

    if pd.notna(row["Short Ratio"]) and row["Short Ratio"] <= ind_av.loc[industry, "Short Ratio"]:
        score += 1

    

    

    if  1.0 <= pd.notna(row["Recommendation"]) <= 1.5 :
        score += 1
    
    if  1.5 < pd.notna(row["Recommendation"]) <= 2 :
        score += 0.5

    if pd.notna(row["ROE"]) and row["ROE"] >= ind_av.loc[industry, "ROE"]:
        score += 1
    if (
        pd.notna(row["Free Cash Flow"])
        and row["Free Cash Flow"] >= ind_av.loc[industry, "Free Cash Flow"]
    ):
        score += 1
    if (
        pd.notna(row["Revenue Growth"])
        and row["Revenue Growth"] >= ind_av.loc[industry, "Revenue Growth"]
    ):
        score += 1
    return score


def prep_datasheet(
    region_dict: dict, output_file: str = os.getcwd() + r"\\Stock_multiples.xlsx"
) -> None:
    """
    Collects financial data for a list of stock tickers grouped by region, computes industry averages,
    scores each stock based on financial performance relative to its industry, and writes the results
    to an Excel file with separate sheets for each region.

    Parameters:
        region_dict (dict): A dictionary where keys are region names (e.g., 'US', 'Euro') and values are
                            lists of stock ticker symbols corresponding to that region.
        output_file (str): The name of the Excel file to write the results to. Defaults to 'Stock_multiples.xlsx'.

    Process:
        - For each region:
            - Retrieves financial metrics for each ticker using `get_info`.
            - Filters out stocks with insufficient data (more than 2 missing key metrics).
            - Computes industry averages for each sector.
            - Scores each stock based on how it compares to its sector's averages.
            - Writes the sorted stock data and industry averages to separate sheets in the Excel file.

    Output:
        An Excel file with the following structure:
            - One sheet per region containing scored stock data sorted by performance.
            - One sheet per region containing industry average metrics.

    Returns:
        None
    """
    writer = pd.ExcelWriter(output_file, engine="openpyxl")
    for region in region_dict:
        print(f"Preparing tickers from {region}")
        results = {ticker: get_info(ticker) for ticker in tqdm(region_dict[region])}
        df = pd.DataFrame.from_dict(results, orient="index")
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        filtered_df = df[
            df[["Debt/equity", "PEG", "ROE", "Free Cash Flow", "Revenue Growth",'Recommendation','Forward PE','Trailing PE','Price/Book','Short Ratio']]
            .isna()
            .sum(axis=1)
            < 4
        ].copy()
        industry_av = filtered_df.groupby("Sector").mean(numeric_only=True)

        filtered_df["Score"] = filtered_df.apply(
            score_row, axis=1, ind_av=industry_av
        )

        filtered_df.sort_values(["Score"], ascending=False).to_excel(
            writer, sheet_name=f"{region} stocks", index=True
        )
        industry_av.reset_index().to_excel(
            writer, sheet_name=f"{region} industry", index=False
        )
    writer.close()


def get_ticker_from_name(company_name):
    """
    Retrieves the stock ticker symbol for a given company name using Yahoo Finance's search API.

    Parameters:
        company_name (str): The name of the company to search for.

    Returns:
        str or None: The ticker symbol of the company if found, otherwise None.
    """
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    params = {"q": company_name, "quotesCount": 1, "newsCount": 0}
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(url, params=params, headers=headers)
    data = response.json()

    if "quotes" in data and len(data["quotes"]) > 0:
        quote = data["quotes"][0]
        return quote.get("symbol")
    else:
        return None


# Scraping Stoxx index names is troublesome. Helperfunctions to do this


def download_pdf(url: str) -> str:
    """Download PDF to temporary file and return path"""
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(response.content)
        return tmp_file.name


def clean_company_name(name: str) -> str:
    """Clean company name by removing sector, country, and weight info"""
    cleaned_name = name

    # Remove common patterns that aren't part of company names
    patterns_to_remove = [
        "Consumer Products & Services",
        "Personal Care, Drug & Grocery Stores",
        "Food, Beverage & Tobacco",
        "Utilities",
        "Retail",
        "Real Estate",
        "Construction & Materials",
        "Health Care" "Technology",
        "Energy",
        "Telecommunications",
        "France",
        "Germany",
        "Great Britain",
        "Switzerland",
        "Italy",
        "Sweden",
        "Denmark",
        "Poland",
        "Spain",
        "Netherlands",
        "Belgium",
        "Finland",
        "Ireland",
        "Austria",
        "Portugal",
        "Luxembourg",
    ]

    for pattern in patterns_to_remove:
        cleaned_name = cleaned_name.replace(pattern, "").strip()

    # Remove trailing numbers (weights) using regex
    cleaned_name = re.sub(r"\s+\d+\.\d+$", "", cleaned_name).strip()

    return cleaned_name


def is_valid_company_name(name: str) -> bool:
    """Check if the name appears to be a valid company name"""
    return (
        len(name) > 2
        and not name.replace(".", "").replace("-", "").replace(" ", "").isdigit()
        and not name.lower()
        in [
            "name",
            "company",
            "symbol",
            "isin",
            "index",
            "weight",
            "sector",
            "components1",
        ]
        and not name.startswith("DE")  # Skip ISIN codes
        and not name.startswith("NL")
        and not name.startswith("FR")
        and not name.startswith("IT")
        and not name.startswith("ES")
        and not name.startswith("BE")
        and not name.startswith("FI")
        and not name.startswith("IE")
        and not name.startswith("AT")
        and not name.startswith("PT")
        and not name.startswith("LU")
        and "%" not in name
        and "INDEX" not in name.upper()
        and "STOXX" not in name.upper()
    )


def extract_euro_stoxx_companies(pdf_path: str) -> List[str]:
    """Extract EURO STOXX company names using pdfplumber"""
    company_names = []

    with pdfplumber.open(pdf_path) as pdf:

        for page_num, page in enumerate(pdf.pages):

            # Extract tables from the page
            tables = page.extract_tables()

            if tables:

                for table_idx, table in enumerate(tables):
                    if table and len(table) > 0:

                        # Look through each column for company names
                        for col_idx in range(len(table[0]) if table[0] else 0):
                            col_data = []

                            # Extract all values from this column
                            for row in table:
                                if col_idx < len(row) and row[col_idx]:
                                    cell_value = str(row[col_idx]).strip()
                                    if cell_value:
                                        col_data.append(cell_value)

                            # Check if this column contains company names
                            if len(col_data) >= 10:  # Should have many companies
                                # Clean and filter company names
                                potential_companies = []
                                for name in col_data:
                                    cleaned_name = clean_company_name(name)

                                    if is_valid_company_name(cleaned_name):
                                        potential_companies.append(cleaned_name)

                                # If we found a good number of potential companies, use this column
                                if len(potential_companies) >= 20:

                                    company_names.extend(potential_companies)
                                    break  # Found companies, move to next table

            # Also try extracting text directly and looking for patterns
            text = page.extract_text()
            if text and not company_names:  # Only if we haven't found companies yet
                lines = text.split("\n")
                text_companies = []

                for line in lines:
                    line = line.strip()
                    cleaned_line = clean_company_name(line)

                    # Look for lines that might be company names
                    if (
                        len(cleaned_line) > 3
                        and is_valid_company_name(cleaned_line)
                        and any(
                            word[0].isupper() for word in cleaned_line.split() if word
                        )
                    ):  # Has capitalized words
                        text_companies.append(cleaned_line)

                if len(text_companies) >= 20:
                    company_names.extend(text_companies)

    # Remove duplicates while preserving order
    unique_companies = []
    seen = set()
    for name in company_names:
        if name not in seen:
            unique_companies.append(name)
            seen.add(name)

    return unique_companies


def get_euro_stoxx_companies(index_code: str) -> List[str]:
    """
    Main function to extract EURO STOXX 50 companies from PDF

    Args:
        pdf_url: URL of the EURO STOXX 50 components PDF

    Returns:
        List of company names
    """
    pdf_url = (
        f"https://www.stoxx.com/document/Bookmarks/CurrentComponents/{index_code}.pdf"
    )

    # Extract tables from the PDF without treating the first row as header
    tables = tabula.read_pdf(
        pdf_url, pages="all", multiple_tables=True, pandas_options={"header": None}
    )
    if len(tables) == 0:
        try:
            # Download PDF
            pdf_path = download_pdf(pdf_url)

            # Extract company names
            companies = extract_euro_stoxx_companies(pdf_path)

            # Display results

            if companies:
                for company in companies:
                    eu_name_list.append(company)


            return companies

        except Exception as e:
            print(f"Error: {e}")
            return []

        finally:
            # Clean up temporary file
            if pdf_path and os.path.exists(pdf_path):
                os.unlink(pdf_path)
    else:
        company_names = tables[0].iloc[:, 0].dropna().tolist()
        for name in company_names:
            eu_name_list.append(name)


if __name__ == "__main__":
    # Europan tickers
    eu_name_list = []

    SXXP = [
        "SX5GT",
        "SXDP",
        "SXNP",
        "SX3P",
        "SX7P",
        "SX8P",
        "SXQP",
        "SXIP",
        "SXEP",
        "SX4P",
        "SX6P",
        "SXRP",
        "SXKP", #Currently not working
        "SXOP",
        "SXFP",
        "SX86P",
        "SXAP",
        "SXPP",
        "SXMP",
        "SXTP",
    ]

print('Scraping European tickers')
for index in SXXP:
    get_euro_stoxx_companies(index)


eu_name_filtered = set(eu_name_list)
eurtickerlist = []
for name in eu_name_filtered:
    ticker = get_ticker_from_name(name)
    if ticker is not None:
        eurtickerlist.append(ticker)

# US ticker list
print('Scraping US tickers')
sptickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[
    0
].drop(
    [
        "Security",
        "GICS Sector",
        "GICS Sub-Industry",
        "Headquarters Location",
        "Date added",
        "CIK",
        "Founded",
    ],
    axis=1,
)
sp500 = sptickers["Symbol"].tolist()

dowtickers = pd.read_html("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average")[
    2
].drop(
    ["Company", "Exchange", "Industry", "Date added", "Notes", "Index weighting"],
    axis=1,
)
dow = dowtickers["Symbol"].tolist()

nasdaqtickers = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4].drop(
    ["Company", "GICS Sector", "GICS Sub-Industry"],
    axis=1,
)
nasdaq = nasdaqtickers["Ticker"].to_list()

ustickerlist = set(nasdaq + dow + sp500)

regions = {"US": ustickerlist, "Euro": eurtickerlist}

print('Initiating data retreival and formatting')
prep_datasheet(regions) 