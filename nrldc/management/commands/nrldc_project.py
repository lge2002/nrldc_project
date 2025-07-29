import requests
import datetime
import os
import tabula
import pandas as pd
import json # Import the json module

from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = 'Download today\'s NRDC report and extract tables 2(A) and 2(C) to a single JSON file'

    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0, debug_table_name="Unknown Table"):
        """
        Extracts a sub-table from a DataFrame based on start and optional end markers.
        Handles multi-level headers by explicitly taking a specified number of rows after the start marker
        as header rows and combining them intelligently.

        Args:
            df (pd.DataFrame): The DataFrame to search within.
            start_marker (str): The regex pattern to identify the start of the sub-table (usually the table title).
            end_marker (str, optional): The regex pattern to identify the end of the sub-table.
                                        If None, extracts from start_marker to the end of the DataFrame.
            header_row_count (int): The number of rows immediately following the start_marker (or actual data start)
                                    that constitute the header. These rows will be combined to form column names.
            debug_table_name (str): A name for the table being processed, used in debug prints.

        Returns:
            pd.DataFrame or None: The extracted sub-table, or None if the start marker is not found.
        """
        start_idx = None
        end_idx = None

        # Find the start index (the row containing the table title)
        for i, row in df.iterrows():
            if row.astype(str).str.strip().str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.stdout.write(self.style.WARNING(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}."))
            return None

        # Determine the end index
        if end_marker:
            for i in range(start_idx + 1, len(df)):
                if df.iloc[i].astype(str).str.strip().str.contains(end_marker, regex=True, na=False, case=False).any():
                    end_idx = i
                    break

        # Extract the initial raw sub-table including the title and potential header rows
        if end_idx is not None:
            raw_sub_df = df.iloc[start_idx:end_idx].copy().reset_index(drop=True)
        else:
            raw_sub_df = df.iloc[start_idx:].copy().reset_index(drop=True)

        # Adjust start_idx for actual data based on header_row_count
        # The first row (index 0) of raw_sub_df is the table title.
        # Actual headers start from index 1. Data starts after title row + header_row_count.
        data_start_row_in_raw_sub_df = 1 + header_row_count

        if header_row_count > 0 and len(raw_sub_df) >= data_start_row_in_raw_sub_df: # Ensure enough rows for headers
            # Extract header rows
            headers_df = raw_sub_df.iloc[1 : data_start_row_in_raw_sub_df]

            new_columns = []
            if header_row_count == 1:
                new_columns = headers_df.iloc[0].astype(str).str.strip().tolist()
            elif header_row_count == 2:
                raw_top_header = headers_df.iloc[0].astype(str).str.strip()
                raw_bottom_header = headers_df.iloc[1].astype(str).str.strip()

                # Manually combine headers based on observed structure for Table 2(A)
                if debug_table_name == "Table 2(A)":
                    new_columns.append(raw_top_header.iloc[0])

                    generation_prefix = raw_top_header.iloc[1]
                    for col_idx in range(1, 8):
                        sub_category = raw_bottom_header.iloc[col_idx]
                        if pd.isna(sub_category) or sub_category == 'nan':
                            new_columns.append(generation_prefix)
                        else:
                            new_columns.append(f"{generation_prefix} {sub_category}".strip())

                    top_level_categories_for_units = raw_top_header.iloc[2:8].tolist()
                    units = raw_bottom_header.iloc[8:14].tolist()

                    for i in range(len(top_level_categories_for_units)):
                        t_cat = top_level_categories_for_units[i]
                        unit = units[i]
                        if pd.isna(unit) or unit == 'nan':
                            new_columns.append(t_cat)
                        else:
                            new_columns.append(f"{t_cat} {unit}".strip())
                
                elif debug_table_name == "Table 2(C)":
                    temp_header_df = pd.DataFrame([raw_top_header, raw_bottom_header])
                    temp_header_df.iloc[0] = temp_header_df.iloc[0].ffill()

                    for col_idx in range(len(temp_header_df.columns)):
                        t_val = temp_header_df.iloc[0, col_idx]
                        b_val = temp_header_df.iloc[1, col_idx]

                        if pd.isna(b_val) or b_val == 'nan':
                            new_columns.append(t_val)
                        elif pd.isna(t_val) or t_val == 'nan':
                            new_columns.append(b_val)
                        elif b_val.startswith(t_val):
                            new_columns.append(b_val)
                        else:
                            new_columns.append(f"{t_val} {b_val}".strip())
                else:
                    self.stdout.write(self.style.WARNING(f"⚠️ Custom header combination logic not defined for {debug_table_name}. Falling back to default."))
                    top_header_ffill = raw_top_header.ffill().astype(str).str.strip()
                    for idx in range(len(top_header_ffill)):
                        t_col = top_header_ffill.iloc[idx]
                        b_col = raw_bottom_header.iloc[idx]
                        if pd.isna(b_col) or b_col == 'nan':
                            new_columns.append(t_col)
                        elif t_col and not b_col.startswith(t_col):
                            new_columns.append(f"{t_col} {b_col}".strip())
                        else:
                            new_columns.append(b_col)

            else:
                self.stdout.write(self.style.WARNING(f"⚠️ Unsupported header_row_count: {header_row_count} for {debug_table_name}. Header processing skipped."))
                new_columns = None

            if new_columns is not None:
                expected_data_cols = raw_sub_df.shape[1]
                if len(new_columns) < expected_data_cols:
                    new_columns.extend([f"Unnamed_Col_{i}" for i in range(len(new_columns), expected_data_cols)])
                elif len(new_columns) > expected_data_cols:
                    new_columns = new_columns[:expected_data_cols]

                sub_df_data = raw_sub_df.iloc[data_start_row_in_raw_sub_df:].copy()
                sub_df_data.columns = new_columns
                sub_df_data = sub_df_data.loc[:, ~sub_df_data.columns.duplicated()]
                sub_df_data.columns = sub_df_data.columns.astype(str).str.strip()
                sub_df_data.columns = sub_df_data.columns.str.replace(r'\s*\r\s*', ' ', regex=True).str.strip()

                sub_df_data = sub_df_data.dropna(axis=0, how='all')
                return sub_df_data.dropna(axis=1, how='all')
            else:
                return raw_sub_df.iloc[data_start_row_in_raw_sub_df:].dropna(axis=1, how='all')
        else:
            return raw_sub_df.iloc[1:].dropna(axis=1, how='all')


    def extract_tables_from_pdf(self, pdf_path, output_dir):
        """
        Extracts specific tables (2A and 2C) from a PDF and saves them as a single JSON file.

        Args:
            pdf_path (str): The path to the PDF file.
            output_dir (str): The directory to save the extracted JSON file.

        Raises:
            CommandError: If PDF extraction fails or no tables are found.
        """
        self.stdout.write("🔍 Extracting tables from PDF...")

        try:
            tables = tabula.read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                pandas_options={'header': None},
                lattice=True
            )
        except Exception as e:
            raise CommandError(f"❌ Tabula extraction failed: {e}")

        if not tables:
            raise CommandError("❌ No tables found in the PDF.")

        self.stdout.write(self.style.SUCCESS(f"✅ Found {len(tables)} tables."))
        
        # Combine all extracted tables into a single DataFrame for comprehensive searching
        all_content_df = pd.DataFrame()
        for df in tables:
            all_content_df = pd.concat([all_content_df, df], ignore_index=True)

        # Clean the combined DataFrame once
        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all').dropna(axis=1, how='all')

        combined_json_data = {}

        # --- Extract Table 2(A) ---
        sub_2A = self.extract_subtable_by_markers(
            all_content_df_cleaned, # Search in the combined DataFrame
            start_marker=r".*2\s*\(A\)\s*State's\s*Load\s*Deails.*",
            end_marker=r"2\s*\(B\)\s*State\s*Demand\s*Met\s*\(Peak\s*and\s*off-Peak\s*Hrs\)",
            header_row_count=2, # Expecting 2 header rows after the title row
            debug_table_name="Table 2(A)"
        )
        if sub_2A is not None:
            combined_json_data['table_2A'] = sub_2A.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(A) extracted for combined JSON."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(A) not found or extraction failed."))

        # --- Extract Table 2(C) ---
        sub_2C = self.extract_subtable_by_markers(
            all_content_df_cleaned, # Search in the combined DataFrame
            start_marker=r"2\s*\(C\)\s*State's\s*Demand\s*Met\s*in\s*MWs.*",
            end_marker=r"3\s*\(A\)\s*StateEntities\s*Generation:",
            header_row_count=2, # Assuming 2 header rows for Table 2(C) as well
            debug_table_name="Table 2(C)"
        )
        if sub_2C is not None:
            combined_json_data['table_2C'] = sub_2C.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) extracted for combined JSON."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(C) not found or extraction failed."))

        # --- Save Combined JSON ---
        if combined_json_data:
            combined_json_path = os.path.join(output_dir, 'nrdc_report_tables.json')
            with open(combined_json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_json_data, f, indent=4, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"✅ Combined tables saved to: {combined_json_path}"))
        else:
            self.stdout.write(self.style.WARNING("⚠️ No tables were successfully extracted to create a combined JSON file."))


    def handle(self, *args, **options):
        """
        Main entry point for the Django management command.
        Downloads the NRDC report and initiates table extraction.
        """
        today = datetime.date.today().strftime("%Y-%m-%d")

        url = f"https://nrldc.in/get-documents-list/111?start_date={today}&end_date={today}"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://nrldc.in/reports/daily-psp",
        }

        self.stdout.write(f"🌐 Fetching NRDC report metadata for {today}...")
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise CommandError(f"❌ Error fetching NRDC metadata: {e}")

        try:
            data = response.json()
        except Exception as e:
            raise CommandError(f"❌ Failed to parse JSON response: {e}")

        if data.get("recordsFiltered", 0) == 0:
            raise CommandError("❌ No report available for today.")

        file_info = data["data"][0]
        file_name = file_info["file_name"]
        title = file_info["title"]

        download_url = f"https://nrldc.in/download-file?any=Reports%2FDaily%2FDaily%20PSP%20Report%2F{file_name}"

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_dir = os.path.join("downloads", f"report_{timestamp}")
        os.makedirs(output_dir, exist_ok=True)
        self.stdout.write(f"📁 Created output directory: {output_dir}")

        pdf_path = os.path.join(output_dir, f"{title}.pdf")
        self.stdout.write(f"⬇️ Attempting to download PDF to: {pdf_path}")

        try:
            pdf_response = requests.get(download_url, headers=headers)
            pdf_response.raise_for_status()
            with open(pdf_path, "wb") as f:
                f.write(pdf_response.content)
            self.stdout.write(self.style.SUCCESS(f"✅ Downloaded report to: {pdf_path}"))
        except Exception as e:
            raise CommandError(f"❌ Failed to download PDF: {e}")

        self.extract_tables_from_pdf(pdf_path, output_dir)
