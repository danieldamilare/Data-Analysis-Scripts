#!/usr/bin/env python
import pandas as pd
import numpy as np
import os
import sys
import logging
import re
from typing import Tuple, List
from dateutil import parser

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler('app.log', mode='a', encoding='utf8')
file_handler.setLevel("INFO")
logger.addHandler(console_handler)
logger.addHandler(file_handler)
formatter = logging.Formatter('\n%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] -%(funcName)s - ====%(message)s===\n')
console_handler.setFormatter(formatter)
formatter = logging.Formatter('\n%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] -%(funcName)s - %(message)s\n')
file_handler.setFormatter(formatter)


def remove_illegal_chars(value):
    if pd.isna(value):
        return np.nan

    if isinstance(value, str):
        value = value.replace('\x00', '')
        value = value.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        value = ''.join(char for char in value if ord(char) >= 32)
    return value

def get_file_list(path: str) -> List:
    res = []
    for file in os.listdir(path):
        new_path = os.path.join(path, file)
        logger.debug(f"Reading File: {new_path}")
        if os.path.isdir(new_path):
            res.extend(get_file_list(new_path))
        elif file.endswith('.xlsx') and not file.startswith('~$') and not file.startswith('.'):
            res.append(new_path)
    return res

def _clean_str(name: str):
    cleaned = ' '.join(
                x for x in 
                (re.sub(r'[^A-Za-z]', ' ', str(name))
                .split(' ')) if x
            ).upper()
    return cleaned


def extract_facility_name_header(df: pd.DataFrame) -> Tuple[str, int]:
    phc_name_row = df[df.iloc[:, 0].astype(str).map(_clean_str) == 'PHC NAME']
    if (len(phc_name_row) != 1):
        # logger.debug(f"Error on facility with header {df.head()}")
        raise ValueError("Can't extract facility name. Please check format")
    phc_name_row = phc_name_row.dropna(axis=1)

    phc_name = None
    for val in phc_name_row.iloc[0, 1:]:  # skip first cell
        if pd.notna(val) and str(val).strip():
            phc_name = str(val).upper()
            break

    if not phc_name:
        raise ValueError("Can't extract Facility name. Please Check format")

    logger.debug(f"PHC Name: {phc_name}")
    logger.debug("Extracting Facility header")

    needed_header_column = {_clean_str(x) for x in ['S/N', 'SURNAME', 'First Name', 'OUTCOME']}
    logger.debug(f"needed header ={needed_header_column}")

    header_idx = -1
    for idx, row in df.iterrows():
        row_value = set(_clean_str(x) for x in row.values)
        if len(needed_header_column.intersection(row_value)) >= 3:
            # logger.debug(f"\n\nfind row: {row}\n\n")
            header_idx = idx
            break

    if header_idx == -1:
        logger.debug("Can't Find Facility header after searching all through")
        raise ValueError("SpreadSheet has no facility header")
    return phc_name, header_idx


def sanitize_header_columns(header: List) -> List:
    header = [_clean_str(h) for h in header]

    diag_label = iter(['MATERNAL DIAGNOSIS', 'CHILD DIAGNOSIS', 'OPD DIAGNOSIS'])
    care_label = iter(['MATERNAL CARE', 'CHILD CARE', 'OPD CARE'])
    outcome_label = iter(['TREATMENT OUTCOME'])

    new_header = []
    header_mapping = {
            r'BIRTH|DOB': 'DOB',
            r'DATE': 'VISIT DATE',
            r'OUTCOME': 'TREATMENT OUTCOME',
            r'REFERRAL': 'REFERRAL',
            r'IDENTIFICATION': 'POLICY NUMBER', 
            r'PHONE': 'PHONE NUMBER',
            r'SURNAME': 'SURNAME',
            r'FIRST': 'FIRST NAME',
            r'S N': 'S/N',
            r'SEX': 'SEX'
      }

    for h in header:
        if re.search(r'REASON|DIAGNOSIS', h):
            new_header.append(next(diag_label, f'Extra_Diagnosis_{h}'))
        elif re.search(r'CARE', h):
            new_header.append(next(care_label, f'Extra_Care_{h}'))
        elif re.search(r'OUTCOME', h):
            new_header.append(next(outcome_label, f'Extra_outcome_{h}'))
        else:
            match = next((val for regex, val in header_mapping.items() if re.search(regex, h)), h)
            new_header.append(match)

    seen = {}
    deduped = []
    for h in new_header:
        if h in seen:
            seen[h] += 1
            deduped.append(f'Extra_{h}_{seen[h]}')
        else:
            seen[h] = 0
            deduped.append(h)

    return deduped


def parse_date(date_str, min_year=1900, max_year=2026):
    try:
        result = pd.to_datetime(date_str, errors='raise')
        if min_year <= result.year <= max_year:
            return result
    except Exception:
        pass

    date_str = str(date_str).strip().upper()
    date_str = re.sub(r'00:00:00', '', date_str).strip()
    date_str = re.sub(r'\s+', '', date_str)

    if current := re.search(r'(0?\d{1,2})[-\\/|]/?(0?\d{1,2})[-\\/|]/?(0?\d{2,4})', date_str):
        day, month, year = int(current.group(1)), int(current.group(2)), current.group(3)

        if month > 12 and day <= 12:
            month, day = day, month

        if len(year) == 2:
            first_year = int('20' + year)
            second_year = int('19' + year)
            year = first_year if min_year <= first_year <= max_year else second_year if min_year <= second_year <= max_year else None
            if year is None:
                return np.nan
        elif len(year) == 3:
            return np.nan
        else:
            year = int(year)

        if not (min_year <= year <= max_year):
            return np.nan

        try:
            return pd.Timestamp(year=year, month=month, day=day)
        except Exception:
            return np.nan

    if current := re.search(r'(\d+).*(?:YEARS?|YRS?)', date_str):
        birth_year = max_year - int(current.group(1))
        if birth_year < min_year:
            return np.nan
        return pd.Timestamp(year=birth_year, month=1, day=1)

    if current := re.search(r'(?:MONTHS?|MNTHS?|DAYS?|DY)', date_str):
                return pd.Timestamp(year=max_year, month = 1, day = 1)
    if current := re.match(r'^(\d+)(?:\.\d+)?$', date_str):
        try:
            extract = pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(current.group(1)))
            if min_year <= extract.year <= max_year:
                return extract
        except Exception:
            pass
    if current := re.search(r'(\d+)[-\\/|](\d{5,6})', re.sub('[^0-9\\/-|]', '', date_str)):
        day = int(current.group(1))
        group2 = current.group(2)
        if len(current.group(2)) == 5:
            month, year = int(group2[0]), int(group2[1:])
        else:
            month, year = int(group2[:2]), int(group2[2:])
        try:
            return pd.Timestamp(year=year, month=month, day= day)
        except Exception as e:
            return np.nan
    return np.nan


def merge_spilled_diagnosis(df: pd.DataFrame):
    starting_index = df[df[['SURNAME', 'FIRST NAME', 'RDIAGNOSIS']].notna().all(axis=1)].index.to_list()
    starting_index.append(len(df))
    for i in range(0, len(starting_index) - 1):
        idx = starting_index[i]
        diagnosis = [str(df.loc[idx, 'RDIAGNOSIS']).strip()]
        treatment = [str(df.loc[idx, 'RCARE']).strip()]
        for j in range(idx + 1, starting_index[i + 1]):
            cur_diag = str(df.loc[j, 'RDIAGNOSIS']).strip()
            cur_care = str(df.loc[j, 'RCARE']).strip()
            if not (cur_diag or cur_care):
                break
            if cur_diag:
                diagnosis.append(cur_diag)
            if cur_care:
                treatment.append(cur_care)
        df.loc[idx, 'RDIAGNOSIS'] = ' '.join(diagnosis)
        df.loc[idx, 'RCARE'] = ' '.join(treatment)
    return df.loc[starting_index[:-1]].reset_index(drop=True)


def get_month_date(file: str):
    matching = re.search(r'(?P<month>(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*)\s*(?P<year>202\d)', file.upper())
    if not matching:
        matching = re.search(r'(?P<year>202\d)\s*(?P<month>(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\w*)', file.upper())
    if not matching:
        return None
    return pd.Timestamp(f"{matching.group('month')} {matching.group('year')}")


def fix_date(df: pd.DataFrame, month_date: pd.Timestamp):
    df['VISIT DATE'] = df['VISIT DATE'].map(lambda x: parse_date(x, min_year=month_date.year - 1, max_year=month_date.year))
    df['VISIT DATE'] = df['VISIT DATE'].ffill().bfill()
    df['VISIT DATE'] = df['VISIT DATE'].fillna(month_date)  # last resort
    df['DOB'] = df['DOB'].map(parse_date)
    return df


def process_bhcpf_file(file: str) -> pd.DataFrame:
    logger.debug(f"Processing {file}")
    try:
        sheet_list = pd.read_excel(file, header=None, sheet_name = None, dtype=str, engine='calamine')
    except Exception as e:
        logger.error(f"Can't load {file} into dataframe. Errr: {e}.")
        raise ValueError(e)

    total_facilities_list =  []

    for sheet, df in sheet_list.items():
        if df.empty:
            logger.info(f"Skipping {sheet} in {file} with no data")
            continue

        try:
            phc_name, header_row =  extract_facility_name_header(df)
            header_values = sanitize_header_columns(df.iloc[header_row].values)
            data_start = header_row + 1
            processed_df = df.iloc[data_start:, :].copy()
            processed_df.columns = header_values
            processed_df = processed_df.map(lambda x: x.strip() if isinstance(x, str) else x)
            processed_df = processed_df.loc[:, ~processed_df.columns.isin(['NAN', '', 'NONE'])]
            processed_df['FACILITY'] = phc_name

            # normalized everything to either 16 or 12
            if 'PHONE NUMBER' not in processed_df.columns:
                processed_df['PHONE NUMBER'] = '08000000000'
            if 'POLICY NUMBER' not in processed_df.columns:
                processed_df['POLICY NUMBER'] = 'MISSING'

            care_columns = processed_df.columns.astype(str).str.contains('CARE')
            care = processed_df.loc[:, care_columns]
            diagnosis_column = processed_df.columns.astype(str).str.contains('DIAGNOSIS')
            diagnosis = processed_df.loc[:, diagnosis_column]

            try:
                processed_df['RDIAGNOSIS'] = diagnosis.bfill(axis=1).iloc[:, 0]
                processed_df['RCARE'] = care.bfill(axis=1).iloc[:, 0]
            except Exception as e:
                print("Can't find any diagnosis column in sheet")
                continue

            processed_df = processed_df[["FACILITY", 'VISIT DATE', 'SURNAME',
                                         'FIRST NAME', 'DOB', 'SEX', 'PHONE NUMBER', 
                                         'POLICY NUMBER', 'RDIAGNOSIS', 'RCARE', 'TREATMENT OUTCOME']]

            processed_df = processed_df.map(remove_illegal_chars)
            processed_df = processed_df.dropna(how='all')
            processed_df = merge_spilled_diagnosis(processed_df)

            logger.info(f"Columns of df: {processed_df.columns}")
            processed_df['SURNAME'] = processed_df['SURNAME'].str.strip()
            processed_df['FIRST NAME'] = processed_df['FIRST NAME'].str.strip()
            processed_df = processed_df.dropna(subset = ['SURNAME', 'FIRST NAME'])

            month_date = get_month_date(file)

            processed_df = fix_date(processed_df, month_date)

            processed_df['DOB'] = processed_df['DOB'].ffill()
            processed_df['VISIT DATE'] = processed_df['VISIT DATE'].ffill()

            processed_df = processed_df[
                ~processed_df['SURNAME'].astype(str).map(_clean_str).isin(['SURNAME', 'LAST NAME', 'FAMILY NAME'])
            ]
            processed_df = processed_df[
                ~processed_df['VISIT DATE'].astype(str).map(_clean_str).str.contains(r'DATE|DD MM', na=False)
            ]
            processed_df = processed_df[
                ~processed_df['DOB'].astype(str).map(_clean_str).str.contains(r'DATE|DD MM', na=False)
            ]

            # Each sheet doesn't have NIN. just put a placeholder
            processed_df['NIN'] = '00000000000'
            processed_df['FILENAME'] = file
            processed_df['SHEET'] = sheet

            processed_df = processed_df.dropna(how='all')
            if not processed_df.empty:
                total_facilities_list.append(processed_df)

        except Exception as e:
            logger.error(f"Error while trying to process sheet {sheet} in {file}: {e}")
            continue

    if not total_facilities_list:
        return pd.DataFrame([])
    return pd.concat(total_facilities_list, ignore_index=True)



def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <scheme> <folder path>", file = sys.stderr)
        sys.exit()
    folder_path = sys.argv[2]
    scheme = str(sys.argv[1])

    if scheme.upper() == 'BHCPF':
        logger.info(f"Getting Files from path: {folder_path}")
        file_list = get_file_list(folder_path)

        results = []
        for f in file_list:
            try:
                df = process_bhcpf_file(f)
                if not df.empty:
                    results.append(df)
            except Exception as e:
                # This captures the ValueError and keeps the loop running
                logger.critical(f"FATAL SKIP: Skipping {f} due to corruption: {e}")
                continue

        if not results:
            logger.critical("No data extracted from any file.")
            sys.exit(1)

        total_dataframe = pd.concat(results, ignore_index=True)
        total_dataframe = total_dataframe.map(remove_illegal_chars)
        logger.info(f"Total data accumulated: {len(total_dataframe)}")
        unique_facility = pd.Series(total_dataframe['FACILITY'].unique())
        logger.info(f"Total unique facility: len(unique_facility)")
        total_dataframe.to_excel("temp.xlsx", index=False)
        unique_facility.to_excel('./facility_list.xlsx')
if __name__ == '__main__':
    main()
