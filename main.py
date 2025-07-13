# messy_date_cleaner.py
import pandas as pd
import numpy as np
from dateutil.parser import parse
import re

# --- CONFIGURATION ---
INPUT_FILE = "messy_data.csv"     # Your CSV filename
DATE_COLUMN = "event_date"         # Column with messy dates (case-insensitive)
OUTPUT_FILE = "cleaned_dates.csv"  # Where to save results
DAYFIRST = False                   # Set True for DD/MM formats
# --- END CONFIGURATION ---

def clean_dates(df, date_column):
    """
    Processes date column with enterprise-grade validation
    Returns DataFrame with 4 new columns:
    - cleaned_date: Standardized datetime (or NaT)
    - original_format: Raw input value
    - detected_format: What format was recognized
    - is_valid_date: Validation flag
    """
    df_clean = df.copy()
    
    # 1. Find matching column (case-insensitive, ignores spaces)
    col_matches = [col for col in df_clean.columns 
                  if re.sub(r'\W+', '', col.lower()) == re.sub(r'\W+', '', date_column.lower())]
    
    if not col_matches:
        raise ValueError(f"Column '{date_column}' not found in CSV. Available columns: {list(df_clean.columns)}")
    
    target_col = col_matches[0]
    print(f"🔍 Found date column: '{target_col}'")
    
    cleaned_dates = []
    detected_formats = []
    
    for val in df_clean[target_col]:
        # Handle missing values
        if pd.isna(val):
            cleaned_dates.append(pd.NaT)
            detected_formats.append("MISSING")
            continue
            
        str_val = str(val).strip()
        if not str_val:
            cleaned_dates.append(pd.NaT)
            detected_formats.append("EMPTY_STRING")
            continue
            
        try:
            # Parse with locale awareness
            parsed = parse(str_val, fuzzy=True, dayfirst=DAYFIRST)
            cleaned_dates.append(parsed)
            detected_formats.append(parsed.strftime("%Y-%m-%d %H:%M:%S"))
            
        except (ValueError, TypeError):
            cleaned_dates.append(pd.NaT)
            detected_formats.append("UNPARSEABLE")
    
    # Add metadata columns
    df_clean['cleaned_date'] = cleaned_dates
    df_clean['original_format'] = df_clean[target_col].astype(str)
    df_clean['detected_format'] = detected_formats
    df_clean['is_valid_date'] = ~df_clean['cleaned_date'].isna()
    
    return df_clean, target_col

def print_report(df, date_col):
    """Prints diagnostic insights to console"""
    valid_count = df['is_valid_date'].sum()
    total = len(df)
    print(f"\n=== CLEANING REPORT ===")
    print(f"Column processed: '{date_col}'")
    print(f"Valid dates: {valid_count}/{total} ({valid_count/total:.1%})")
    
    # Show format distribution
    print("\nTop formats detected:")
    print(df['detected_format'].value_counts().head(5))
    
    # Show problematic entries
    invalid_df = df[~df['is_valid_date']]
    if not invalid_df.empty:
        print("\n⚠️ Invalid entries samples:")
        print(invalid_df[['original_format', 'detected_format']].head(10))

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print(f"📂 Reading {INPUT_FILE}...")
    try:
        raw_df = pd.read_csv(INPUT_FILE)
        print(f"✅ Loaded {len(raw_df)} rows")
        print(f"Available columns: {list(raw_df.columns)}")
    except FileNotFoundError:
        print(f"❌ Error: File '{INPUT_FILE}' not found!")
        print("Please create sample data with:")
        print('''id,event_date,location
1,"Oct 12, 2023 14:30",NY
2,"2023-10-13T08:00:00",LA
3,"13/10/2023",Chicago
4,"10-14-2023 11 AM",Miami
5,"Friday, October 15 2023",Seattle''')
        exit(1)
    
    print(f"🧹 Cleaning column matching '{DATE_COLUMN}'...")
    try:
        cleaned_df, found_col = clean_dates(raw_df, DATE_COLUMN)
    except ValueError as e:
        print(f"❌ {str(e)}")
        exit(1)
    
    print("\nFirst 5 rows after cleaning:")
    print(cleaned_df[['original_format', 'cleaned_date', 'is_valid_date']].head())
    
    print_report(cleaned_df, found_col)
    
    cleaned_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n💾 Saved cleaned data to '{OUTPUT_FILE}'")
    print("Done! Enjoy your clean dates 🎉")