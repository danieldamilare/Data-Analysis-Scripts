import pandas as pd
import re

def main():
    # 1. Load Data
    df = pd.read_excel('facility_list.xlsx')
    original = df[0]
    
    noise_pattern = 'BHCPFP|CONTINUATION|CONTIUATION|CLINIC|PRIMARY|HEALTH|CENTER|PHCC|PHC|BHC|CHC|JANUARY|SEPTEMBER|2024|2025|2026'
    
    facility = original.map(lambda x: re.sub(noise_pattern, ' ', str(x).upper()))
    facility = facility.map(lambda x: re.sub(r'[^A-Za-z0-9]', ' ', x)) 
    facility = facility.map(lambda x: re.sub(r'\b[0-9]+\b', ' ', x))  
    facility = facility.map(lambda x: ' '.join(z for z in x.split() if len(z) > 2))
    facility = facility.str.strip()

    master_hospital = pd.read_excel('./data/done facilities.xlsx')
    hospital = master_hospital['HOSPITAL']

    mapping_list = {}
    not_found = set()

    for idx, fac in facility.items():
        fac_tokens = sorted( [t for t in fac.split(' ') if len(t) > 2],
                        key=len, reverse=True)
        found = False

        for tok in fac_tokens:
            matching_fac = hospital[hospital.str.contains(tok, case=False, na=False)]
            if len(matching_fac) == 1:
                mapping_list[original.iloc[idx]] = matching_fac.iloc[0]
                found = True
                break

        if not found:
            matching_fac = hospital[hospital.str.contains(re.escape(fac), case=False, na=False)]
            if len(matching_fac) == 1:
                mapping_list[original.iloc[idx]] = matching_fac.iloc[0]
            else:
                not_found.add(original.iloc[idx])
    
    return mapping_list, not_found

if __name__ == "__main__":
    mapping_list, not_found = main()
