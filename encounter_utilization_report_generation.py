import pandas as pd
import zipfile
import numpy as np
import os
from collections import defaultdict
from groq import Groq, RateLimitError, APIStatusError
from google.colab import userdata, files
from typing import List, Dict, Optional
from tqdm import tqdm
from rapidfuzz import fuzz, process
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import json
import time
import threading
import shutil

print ("Loaded Libraries....")
age_order = [ '<1', '1-5', '6-14', '15-19', '20-44', '45-64', '65&AB' ]
sex_order = ['Male', 'Female']
output_filename = 'consolidated_final_report.xlsx'

master_diagnosis_list = [
    'ABSCESS', 'ANEMIA', 'ANEMIA PREGNANCY', 'ANXIETY', 'A. P. H.', 'APPENDICITIS', 'ARTRITIS /RHEUMATISM', 'ASSITED DEV.', 'ASTHMA', 'BITE (HUMAN)', 'BITE (SCORPION)', 'BITE (SNAKE', 'BITE(DOG)', 'BOIL', 'BREACH DEL', 'BRONCHITIS', 'C. C. F.', 'C.V.D.', 'C/S', 'CELLULITIS.', 'CERE.VAS ACC', 'CEREBRAL PALSY',
    'CHICKEN POX', 'CHOLECYSTITIS', 'CHOLERA', 'CONJUCTIVITIS', 'DERMATITIS & OTHER SKIN DIS.', 'DIAB. MELLIT (DM)', 'DIARRHOEA', 'DISLOCATION', 'DRUG REACTION', 'DYSENTARY', 'DYSPLEGIA', 'ECLAMSIA', 'ECTOPIC PREG.', 'ENDOCROUTE/ NUTRITIONAL DIS.', 'EPILESY', 'FIBROID', 'FILARIASIS', 'FRACTURE', 'GASTRO ENT.', 'GINGIVITIS',
    'GUN SHOT', 'HAEMATOMA', 'HAEMORRHAGE', 'HEMORRHOID', 'HEPATITIS', 'HERNIA', 'HIV/AID', 'HYDROCEPHALUS', 'HYPERTENSION (HTN)', 'HYPOGLYCAEMIA', 'HYPOTENSION', 'INJURY', 'INTESTNAL OBS', 'JAUNDICE', 'KERATITIS', 'LIPOMA', 'CYESIS', 'MALARIA', 'MALARIA IN PREGNANCY', 'MASTITIS', 'MEASLES',
    'MENINGITIS', 'MENTAL DISORDERS', 'MIGRAINE', 'MUMPS', 'NEPHRITIS & OTHER KIDNEY DIS.', 'NOMAL DEL.', 'OSTEOMYELITIS', 'OTITIS MED', 'OVARIAN CYST', 'P . P. H', 'PERITONITIS', 'PID', 'PLACENTAL PREVIA', 'PNEUMOMIA', 'POISON', 'PREG. INDUCE HYPERTENSION', 'R.T.A & OTHER ACC', 'REP. TRAC. INFEC. (RTI)', 'RUPTURED UTERINE',
    'SEPSIS', 'SICKLE CELL ANAEMIA', 'STD', 'STILL BIRTH', 'STOMATITIS', 'TETANUS', 'THREATEN ABORTION', 'TINEA CAPITI./CORPORIS', 'TONGUE TIE', 'TONSILITIS', 'TRAUMA', 'TUBERCULOSIS', 'TYPHOID FEVER', 'ULCER', 'UTERINE ECTOMY', 'UTI', 'VESICO-VAGINAL-FISTULA', 'OTHERS'
]

diagnosis_mapping = {} 
lookup_cache = {
    'HTN': 'HYPERTENSION (HTN)',
    'MF': 'MALARIA',
    'MP': 'MALARIA',
    'EF': 'TYPHOID FEVER',
    'URTI': 'REP. TRAC. INFEC. (RTI)',
    "ENTERITIS": "TYPHOID FEVER",
    "ENTERIC": "TYPHOID FEVER",
    "HEADACHE": "MIGRAINE",
    "DEHYDRATION": "OTHERS",
    "PYELONEPHRITIS": "NEPHRITIS & OTHER KIDNEY DIS.",
    'RTI': 'REP. TRAC. INFEC. (RTI)',
    "SPONDYLOSIS": "ARTRITIS /RHEUMATISM",
    "OSTEOARTHRITIS": "ARTRITIS /RHEUMATISM",
    "PUO": "MALARIA",
    "PUD": "ULCER",
    "SCD": "SICKLE CELL ANAEMIA",
    "HBSS": "SICKLE CELL ANAEMIA",
    "RTA": "R.T.A & OTHER ACC",
    "DM": "DIAB. MELLIT (DM)",
    "PYREXIA": "MALARIA",
    "DYSPEPSIA": "ULCER",
    "INSOMNIA": "ANXIETY",
    "NEUROPATHY": "OTHERS",
    "DERMATITIS": "DERMATITIS & OTHER SKIN DIS.",
    "MALARIA FEVER": "MALARIA",

    "PLASMODIASIS": "MALARIA",
    "MYALGIA": "ARTRITIS /RHEUMATISM",
    "ARTHRALGIA": "ARTRITIS /RHEUMATISM",
    "PHARYNGITIS": "REP. TRAC. INFEC. (RTI)",
    "ARI": "REP. TRAC. INFEC. (RTI)",
    "GASTRITIS": "GASTRO ENT.",
    "GASTROENTERITIS": "GASTRO ENT.",
    "STRESS": "ANXIETY",
    "STRESS DISORDER": "ANXIETY",
    "VAGINITIS": "OTHERS",
    "PREGNANCY": "CYESIS",
    "NOMAL DEL": "NOMAL DEL.",
    "S Delivery": "NOMAL DEL."
}

rate_limit_light = threading.Event()
rate_limit_light.set()

def load_client():
    try:
        api_key = userdata.get('GROQ_API_KEY')
        return Groq(api_key=api_key)
    except Exception:
        print("Error: Ensure 'GROQ_API_KEY' is set in Colab Secrets.")
        return None

def cleaned_diagnosis(diagnosis: str):
  diagnosis = str(diagnosis)
  print("cleaning diagnosis", diagnosis)
  return (' '.join(x for x in re.sub(r'[^A-Za-z]', ' ', diagnosis)
                  .split(' ') if x)).upper()

def map_diagnosis():
    global diagnosis_mapping, _CANONICAL_KEYS

    for diagnosis in master_diagnosis_list:
        diagnosis_mapping.update({cleaned_diagnosis(diagnosis): diagnosis})
    lookup_cache.update(diagnosis_mapping)
    _CANONICAL_KEYS = list(diagnosis_mapping.keys())

def sanitize_sheet_name(name):
    name = str(name)
    name = re.sub(r"[\\\/\?\*\[\]\:\']", '', name)
    return name[:31]

def sanitize_excel_value(value):
    if pd.isna(value):
        return np.nan

    if isinstance(value, str):
        value = value.replace('\x00', '')
        value = value.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        value = ''.join(char for char in value if ord(char) >= 32)
    return value

def parse_age_from_string(age_val):
    if pd.isna(age_val): return np.nan
    age_str = str(age_val).upper()
    numbers = re.findall(r'\d+\.?\d*', age_str)
    if not numbers: return np.nan
    age_num = float(numbers[0])
    if 'MTH' in age_str or 'MONTH' in age_str: return age_num / 12
    elif 'DAY' in age_str: return age_num / 365
    else: return age_num


def categorize_age(age_val) -> Optional[str]:
    if pd.isna(age_val):
        return None
    if age_val < 1:    return '<1'
    if age_val <= 5:   return '1-5'
    if age_val <= 14:  return '6-14'
    if age_val <= 19:  return '15-19'
    if age_val <= 44:  return '20-44'
    if age_val <= 64:  return '45-64'
    return '65&AB'


def _find_header_row(raw_df: pd.DataFrame, needed: set) -> Optional[int]:
    def _normalise(val) -> str:
        return (re.sub(r'[^a-z0-9_]', '', str(val).lower().strip().replace(' ', '_')))

    normalised_header = {_normalise(c) for c in raw_df.columns}
    if needed.issubset(normalised_header):
        return 0
    for idx, row in raw_df.iterrows():
        normalised_row = {_normalise(v) for v in row.values}
        if needed.issubset(normalised_row):
            print("found row idx", idx+1)
            return idx + 1

    return None


def load_clean_dataframe(file_path: str) -> Optional[pd.DataFrame]:
    try:
        facility_name = os.path.splitext(os.path.basename(file_path))[0]
        needed_columns = {'age', 'sex', 'diagnosis'}

        raw = pd.read_excel(file_path, header=0)
        header_row = _find_header_row(raw, needed_columns)

        if header_row is None:
            print(f"{file_path} is missing column(s) {needed_columns}, cannot proceed.")
            return None

        df = pd.read_excel(file_path, header=header_row)

        df.dropna(axis=0, how='all', inplace=True)
        df['facility'] = facility_name
        df.columns = (
            df.columns.str.lower()
                      .str.strip()
                      .str.replace(' ', '_')
                      .str.replace('[^a-z0-9_]', '', regex=True)
        )

        df.dropna(axis=1, how='all', inplace=True)
        last_valid = df[list(needed_columns)].notna().any(axis=1)
        df = df[last_valid].reset_index(drop=True)

        if df.empty:
            print(f"{file_path} has no data rows after header. Skipping.")
            return None

        sex_raw = df['sex'].astype(str).str.strip().str.lower()
        missing_sex_mask = sex_raw.isin(['', 'nan', 'none', 'nat'])

        df['sex'] = np.where(sex_raw.str.contains('f'), 'Female', 'Male')
        df.loc[missing_sex_mask, 'sex'] = np.nan
        no_sex_len = len(df[df['sex'].isna()])
        sex_len = len(df[df['sex'].notna()])
        print("no of given sex: ", sex_len, "no of not given sex: ", no_sex_len)
        if sex_len == 0:
          raise ValueError("Refusing to process script with no sex")


        print("Reached here!!!")

        missing_sex_percentage = float(no_sex_len/sex_len)
        if missing_sex_percentage > 0.50:
          raise ValueError("Refusing to process script with missing sex greater than 50%")

        df['age_numeric'] = df['age'].map(parse_age_from_string)
        missing_age_mask = df['age_numeric'].isna()

        no_age_len = len(df[df['age_numeric'].isna()])
        age_len = len(df[df['age_numeric'].notna()])
        print("no of given age: ", age_len, "no of no age: ", no_age_len)

        if age_len == 0:
          raise ValueError("Refussing to process script without age")

        missing_age_percentage = float(no_age_len/age_len)

        if missing_age_percentage > 0.50:
          raise ValueError("Refusing to process script with missing age greater than 30%")

        known_sex = df.loc[~missing_sex_mask, 'sex']
        if missing_sex_mask.any():
            if known_sex.empty:
                weights = {'Male': 0.5, 'Female': 0.5}
            else:
                counts = known_sex.value_counts(normalize=True)
                weights = counts.to_dict()
            choices = np.random.choice(
                list(weights.keys()),
                size=missing_sex_mask.sum(),
                p=list(weights.values())
            )
            df.loc[missing_sex_mask, 'sex'] = choices
            print(f"  Imputed {missing_sex_mask.sum()} missing sex value(s) "
                  f"using distribution {weights}.")


        if missing_age_mask.any():
            age_median = df['age_numeric'].median()

            df.loc[missing_age_mask, 'age_numeric'] = age_median
            print(f"  Imputed {missing_age_mask.sum()} missing age value(s) "
                  f"with median {age_median:.1f} years.")

        df['age'] = df['age_numeric'].map(categorize_age)
        df.drop(columns=['age_numeric'], inplace=True)

        df['age'] = df['age'].astype('category')
        df['sex'] = df['sex'].astype('category')

        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].map(sanitize_excel_value)

        return df
    except Exception as e:
        print(f"An error occurred when loading {file_path}: {e}")
        return None


def get_file_list(path: str) -> List:
    res = []
    for file in os.listdir(path):
        new_path = os.path.join(path, file)
        if os.path.isdir(new_path):
            res.extend(get_file_list(new_path))
        elif file.endswith('.xlsx') and not file.startswith('~$'):
            res.append(new_path)
    return res

_SYSTEM_PROMPT_TEMPLATE = f"""
You are a medical records classifier. Your ONLY job is to map raw diagnosis strings to the exact list below.

DIAGNOSIS LIST (use ONLY these exact strings, character-for-character):
{master_diagnosis_list}

INPUT FORMAT:
You will receive a JSON array of objects, each with an "id" and "text" field:
[{{"id": 0, "text": "HTN/MF"}}, {{"id": 1, "text": "MALARIA + SEPSIS"}}]

OUTPUT FORMAT:
Return a single JSON object with exactly one key "diagnoses", whose value is a list of objects with "id" and "result":
{{"diagnoses": [{{"id": 0, "result": ["HYPERTENSION (HTN)", "MALARIA"]}}, {{"id": 1, "result": ["MALARIA", "SEPSIS"]}}]}}

STRICT RULES:
1. Every term in "result" MUST be copied VERBATIM from the DIAGNOSIS LIST above.
   - WRONG: "HYPERTENSIVE (HNT)", "HYPERTRAINCE", "SPEISIS"
   - RIGHT: "HYPERTENSION (HTN)", "SEPSIS"
2. If no match exists, use "OTHERS" — never invent a name.
3. Your output "diagnoses" list MUST contain exactly one entry per input object, preserving each "id".
4. Do NOT reorder, skip, or merge entries — every input "id" must appear exactly once in the output.
5. No duplicate terms within a single "result" list.
6. Return empty list [] in "result" for blank/whitespace-only input text.
7. Output raw JSON only — no markdown, no backticks, no explanation.
8. The root key must be exactly "diagnoses".

ABBREVIATION MAP:
HTN → HYPERTENSION (HTN)
MF, MP, MALERIA → MALARIA
EF, E/F, ENTERIC, ENTERIC FEVER → TYPHOID FEVER
URTI, RTI, RHINITIS → REP. TRAC. INFEC. (RTI)
PUD, PUDx, P.U.D, PEPTIC ULCER, DYSPEPSIA, EPIGASTRIC PAIN → ULCER
DM, HYPERGLYCEMIA, GLYCEMIC CONTROL → DIAB. MELLIT (DM)
RTA → R.T.A & OTHER ACC
SCD, HBSS → SICKLE CELL ANAEMIA
SPONDYLOSIS, OSTEOARTHRITIS, MYALGIA, BODY PAIN → ARTRITIS /RHEUMATISM
PUO → MALARIA
CVD → C.V.D.
CCF → C. C. F.

BEFORE RESPONDING, VERIFY:
1. My output contains exactly one entry per input "id".
2. No "id" is duplicated or missing.
3. Every term in every "result" list appears verbatim in the DIAGNOSIS LIST.
4. The root JSON key is exactly "diagnoses".
"""

def _classify_diagnoses(diagnosis_list: List[str]) -> List[List[str]]:
    indexed = [{"id": i, "text": t} for i, t in enumerate(diagnosis_list)]
    prompt = (
        f"Classify these {len(diagnosis_list)} diagnoses: {json.dumps(indexed)}\n"
        f"Return a JSON object where 'diagnoses' is a list of objects, each with 'id' and 'result'.\n"
        f"Example output: {{\"diagnoses\": [{{\"id\": 0, \"result\": [\"MALARIA\"]}}, {{\"id\": 1, \"result\": [\"HYPERTENSION (HTN)\", \"SEPSIS\"]}}]}}\n"
        f"The output list MUST contain exactly {len(diagnosis_list)} entries, one per input id."
    )

    max_retries = 10
    for attempt in range(1, max_retries + 1):
        rate_limit_light.wait()
        try:
            response = CLIENT.chat.completions.create(
                model='llama-3.3-70b-versatile',
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT_TEMPLATE},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                response_format={"type": "json_object"},
                stream=False
            )

            raw_content = response.choices[0].message.content

            result_json = json.loads(raw_content)

            raw_list = result_json.get("diagnoses")
            if not isinstance(raw_list, list):
                raise ValueError("'diagnoses' key missing or not a list")
            id_to_result = {}
            for entry in raw_list:
                if not isinstance(entry, dict) or "id" not in entry or "result" not in entry:
                    raise ValueError(f"Malformed entry: {entry}")
                eid = int(entry["id"])
                if eid in id_to_result:
                    raise ValueError(f"Duplicate id {eid} in response")
                id_to_result[eid] = entry["result"]

            expected_ids = set(range(len(diagnosis_list)))
            returned_ids = set(id_to_result.keys())
            if expected_ids != returned_ids:
                missing = expected_ids - returned_ids
                raise ValueError(f"Missing ids in response: {missing}")

            classified = [id_to_result[i] for i in range(len(diagnosis_list))]
            time.sleep(1.5)
            return classified

        except RateLimitError:
            rate_limit_light.clear()
            wait = 10 * attempt
            print(f"Rate limit (attempt {attempt}/{max_retries}). Waiting {wait}s...")
            time.sleep(wait)
            rate_limit_light.set()
        except Exception as e:
            print(f"Error (attempt {attempt}/{max_retries}): {e}. Retrying in 3s...")
            time.sleep(3)

    print(f"All retries failed for batch. Defaulting to OTHERS.")
    return [['OTHERS']] * len(diagnosis_list)

_CANONICAL_KEYS = list(diagnosis_mapping.keys())

def _fuzzy_snap(term: str) -> str:
    term_cleaned = cleaned_diagnosis(term)
    if term_cleaned in lookup_cache:
        val = lookup_cache[term_cleaned]
        return val if isinstance(val, str) else 'OTHERS'

    result = process.extractOne(term_cleaned, _CANONICAL_KEYS, scorer=fuzz.token_set_ratio)
    if result is None:
        return 'OTHERS'

    match, score, _ = result
    if score >= 80:
        return diagnosis_mapping[match]
    print("Can't found a fuzzy match for: ", term, "returning others...")
    return 'OTHERS'

def _fuzzy_snap_list(terms: List[str]) -> List[str]:
  snapped = [_fuzzy_snap(t) for t in terms if t and t.strip()]
  return snapped


def classify_diagnosis(diagnosis: pd.Series, batch_size: int = 10) -> List[List[str]]:
    print("In classify diagnosis")
    values = diagnosis.tolist()
    values_to_search = list({x for x in values if not lookup_cache.get(cleaned_diagnosis(x), "")})
    print(f"cache hit for {len(values) - len(values_to_search)}")

    batches = [values_to_search[i: i + batch_size] for i in range(0, len(values_to_search), batch_size)]

    with ThreadPoolExecutor(max_workers=3) as executor:
          print("Starting executor with 3 workers")
          futures = {executor.submit(_classify_diagnoses, batch): batch for batch in batches}
          for future in as_completed(futures):
            batch = futures[future]
            try:
              classified = future.result()
              if classified is None:
                classified = [['OTHERS']] * len(batch)
            except Exception as e:
              print(f"Batch failed with exception: {e}. Defaulting to OTHERS.")
              classified = [['OTHERS']] * len(batch)
            for raw, proc in zip(batch, classified):
              print("raw: ", raw)
              print("proc: ", proc)
              lookup_cache[cleaned_diagnosis(raw)] = _fuzzy_snap_list(proc)
    print("Ending Executor now!!!!")
    total_classified = [lookup_cache[cleaned_diagnosis(raw)] for raw in values]
    return total_classified


def process_file_list(file_list: List[str]):
    encounter_total = []
    utilization_list = {}
    all_cols = pd.MultiIndex.from_product([age_order, sex_order], names=['age', 'sex'])

    for file in file_list:
        df = load_clean_dataframe(file)
        if df is None:
            continue

        try:
            print(f"Generating encounter analysis for {file}...")
            enc = (
                df.groupby(['facility', 'age', 'sex'], observed=True)
                  .size()
                  .reset_index(name='n')
            )
            enc['n'] = enc['n'].astype(float)
            enc_table = enc.pivot_table(
                index='facility', columns=['age', 'sex'],
                values='n', fill_value=np.nan, observed=True
            )
            enc_table.index.name = 'Facility'
            encounter_total.append(enc_table)
        except Exception as e:
            print(f"Error building encounter table for {file}: {e}. Skipping file.")
            continue

        try:
            classified = classify_diagnosis(df['diagnosis'])
        except SystemError as e:
            print(f"Classification failed for {file}: {e}. Skipping utilization report.")
            continue

        df = df.copy()
        df['classified_diagnosis'] = classified
        df = df.explode('classified_diagnosis')

        if df.empty:
            print(f"No classified diagnoses for {file}. Skipping utilization report.")
            continue

        df['diagnosis'] = df['classified_diagnosis']

        utilization = (
            df.groupby(['age', 'sex', 'diagnosis'], observed=True)
              .size()
              .reset_index(name='n')
        )
        utilization['n'] = utilization['n'].astype(float)

        report_table = utilization.pivot_table(
            index='diagnosis', columns=['age', 'sex'],
            values='n', fill_value=np.nan, observed=True
        )
        report_table = report_table.reindex(columns=all_cols)
        report_table = report_table.reindex(index=master_diagnosis_list)

        report_table[('Total', 'Male')]        = report_table.loc[:, (slice(None), 'Male')].sum(axis=1, min_count=1)
        report_table[('Total', 'Female')]      = report_table.loc[:, (slice(None), 'Female')].sum(axis=1, min_count=1)
        report_table[('Total', 'Grand Total')] = report_table[[('Total', 'Male'), ('Total', 'Female')]].sum(axis=1, min_count=1)
        report_table = report_table.fillna(0)
        facility_name = df['facility'].iloc[0]
        utilization_list[facility_name] = report_table

    if not encounter_total:
        raise ValueError("No valid encounter data was produced from any file.")

    combined_encounter_report = pd.concat(encounter_total)
    combined_encounter_report = combined_encounter_report.reindex(columns=all_cols)
    combined_encounter_report[('GRAND TOTAL', 'Male')]   = combined_encounter_report.loc[:, (slice(None), 'Male')].sum(axis=1, min_count=1)
    combined_encounter_report[('GRAND TOTAL', 'Female')] = combined_encounter_report.loc[:, (slice(None), 'Female')].sum(axis=1, min_count=1)
    combined_encounter_report.loc['GRAND TOTAL(S)']      = combined_encounter_report.sum(min_count=1)
    combined_encounter_report = combined_encounter_report.fillna(0)

    return combined_encounter_report, utilization_list


def save_to_file(encounter_df: pd.DataFrame, utilization_list: Dict):

    used_sheet_names = set()

    try:
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:

            print("Saving Encounter report...")

            encounter_df.to_excel(writer, sheet_name='Encounter Report')
            used_sheet_names.add('Encounter Report')

            print(f"Saving {len(utilization_list)} facility utilization reports...")

            for facility_name, report_df in utilization_list.items():
                base_name = sanitize_sheet_name(facility_name)
                if not base_name:
                    base_name = "Unnamed_Facility"
                sheet_name = base_name
                count = 1

                while sheet_name in used_sheet_names:
                    suffix = f"_{count}"
                    trunc_len = 31 - len(suffix)
                    sheet_name = f"{base_name[:trunc_len]}{suffix}"
                    count += 1

                    if count > 100:
                        print(f" - Collision limit reached for {facility_name}. Using unique index.")
                        sheet_name = f"Facility_{id(report_df) % 10000}"
                        break

                used_sheet_names.add(sheet_name)

                report_df.to_excel(writer, sheet_name=sheet_name)

        print(f"Successfully saved all reports to {output_filename}")

    except Exception as e:
        print(f"Critical Error during file save: {e}")

def run_colab_process():
    print("Loading Client")
    client = load_client()
    if not client:
        print("Can't connect to llm for classification")
        return
    global CLIENT
    CLIENT = client
    map_diagnosis()

    print("Please Upload zip file: ")
    uploaded = files.upload()
    if not uploaded:
      print("You did not upload a file")
      return

    output_loc = os.path.join('/content/', 'temp')
    if os.path.exists(output_loc):
      if os.path.isdir(output_loc):
        shutil.rmtree(output_loc)
      else:
        os.remove(output_loc)

    os.makedirs(output_loc, exist_ok = True)
    filename = list(uploaded.keys())[0]
    print("Zip file loaded..." )

    with zipfile.ZipFile(filename) as zipf:
        zipf.extractall(output_loc)
    print(f"Extracted all files in {filename}")
    file_list = get_file_list(output_loc)

    if not file_list:
        print("No xlsx files found in uploaded zip. Exiting...")
        return
    encounter_df, utilization_list = process_file_list(file_list)

    save_to_file(encounter_df, utilization_list)
    print("Report successfully generated")
    files.download(output_filename)
    shutil.rmtree(output_loc)

if __name__ == '__main__':
  run_colab_process()

