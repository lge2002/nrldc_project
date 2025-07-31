# nrldc/management/commands/nrldc_project.py

import requests
import datetime
import os
import tabula
import pandas as pd
import json

from django.core.management.base import BaseCommand, CommandError
from nrldc.models import Table2AData, Table2CData # NRDCReport is removed from imports

class Command(BaseCommand):
    help = 'Download today\'s NRDC report and extract tables 2(A) and 2(C) to a single JSON file and save to DB'

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

        data_start_row_in_raw_sub_df = 1 + header_row_count

        if header_row_count > 0 and len(raw_sub_df) >= data_start_row_in_raw_sub_df:
            headers_df = raw_sub_df.iloc[1 : data_start_row_in_raw_sub_df]

            new_columns = []
            if header_row_count == 1:
                new_columns = headers_df.iloc[0].astype(str).str.strip().tolist()
            elif header_row_count == 2:
                raw_top_header = headers_df.iloc[0].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')
                raw_bottom_header = headers_df.iloc[1].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')

                if debug_table_name == "Table 2(A)":
                    new_columns = [
                        'State',
                        'Thermal',
                        'Hydro',
                        'Gas/Naptha/Diesel',
                        'Solar',
                        'Wind',
                        'Others(Biomass/Co-gen etc.)',
                        'Total',
                        'Drawal Sch (Net MU)',
                        'Act Drawal (Net MU)',
                        'UI (Net MU)',
                        'Requirement (Net MU)',
                        'Shortage (Net MU)',
                        'Consumption (Net MU)'
                    ]
                elif debug_table_name == "Table 2(C)":
                    new_columns = [
                        'State',
                        'Maximum Demand Met of the day',
                        'Time', # time_max_demand_met
                        'Shortage during maximum demand',
                        'Requirement at maximum demand',
                        'Maximum requirement of the day',
                        'Time.1', # time_max_requirement
                        'Shortage during maximum requirement',
                        'Demand Met at maximum Requirement',
                        'Min Demand Met',
                        'Time.2', # time_min_demand_met
                        'ACE_MAX',
                        'ACE_MIN', # This is where Tabula places the time string for ACE_MAX
                        'Time.3', # This is where Tabula places the numeric ACE_MIN value
                        'Time.4' # This is where Tabula places the time string for ACE_MIN
                    ]
                else:
                    self.stdout.write(self.style.WARNING(f"⚠️ Custom header combination logic not defined for {debug_table_name}. Falling back to generic combination."))
                    for idx in range(raw_top_header.shape[0]):
                        t_col = raw_top_header.iloc[idx].strip()
                        b_col = raw_bottom_header.iloc[idx].strip()

                        if not t_col and not b_col:
                            new_columns.append(f"Unnamed_{idx}")
                        elif not b_col:
                            new_columns.append(t_col)
                        elif not t_col:
                            new_columns.append(b_col)
                        elif not b_col.startswith(t_col):
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

    def _safe_float(self, value):
        """
        Attempts to convert a value to float, handling commas and ensuring it's not a time string.
        Returns None if conversion fails or if the value appears to be a time string.
        """
        if isinstance(value, str):
            value = value.strip()
            # If it contains a colon, it's a time, so it should NOT be converted to float.
            if ':' in value:
                return None
            # Remove commas for numeric conversion
            value = value.replace(',', '')
            # Handle empty strings or non-numeric strings after comma removal
            if not value or value.lower() in ['n/a', '-', 'null', 'nan']:
                return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_string(self, value):
        """Ensures the value is a string or returns None."""
        if pd.isna(value) or value is None:
            return None
        return str(value).strip() if value is not None else None


    def extract_tables_from_pdf(self, pdf_path, output_dir, report_date): # Changed parameter to report_date
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

        all_content_df = pd.DataFrame()
        for df in tables:
            all_content_df = pd.concat([all_content_df, df], ignore_index=True)

        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all')

        combined_json_data = {}

        # --- Extract Table 2(A) ---
        sub_2A = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r".*2\s*\(A\)\s*State's\s*Load\s*Deails.*",
            end_marker=r"2\s*\(B\)\s*State\s*Demand\s*Met\s*\(Peak\s*and\s*off-Peak\s*Hrs\)",
            header_row_count=2,
            debug_table_name="Table 2(A)"
        )
        if sub_2A is not None:
            column_mapping_2A = {
                'State': 'state',
                'Thermal': 'thermal',
                'Hydro': 'hydro',
                'Gas/Naptha/Diesel': 'gas_naptha_diesel',
                'Solar': 'solar',
                'Wind': 'wind',
                'Others(Biomass/Co-gen etc.)': 'other_biomass_co_gen_etc',
                'Total': 'total',
                'Drawal Sch (Net MU)': 'drawal_sch',
                'Act Drawal (Net MU)': 'act_drawal',
                'UI (Net MU)': 'ui',
                'Requirement (Net MU)': 'requirement',
                'Shortage (Net MU)': 'shortage',
                'Consumption (Net MU)': 'consumption',
            }
            sub_2A_renamed = sub_2A.rename(columns=column_mapping_2A)
            sub_2A_filtered = sub_2A_renamed[[col for col in column_mapping_2A.values() if col in sub_2A_renamed.columns]]

            # self.stdout.write(f"\n--- Debugging Table 2A Filtered Data ---")
            # self.stdout.write(f"Columns: {sub_2A_filtered.columns.tolist()}")
            # self.stdout.write(f"Sample data:\n{sub_2A_filtered.head().to_string()}") # Use to_string for better display
            # self.stdout.write(f"----------------------------------------\n")

            combined_json_data['table_2A'] = sub_2A_filtered.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(A) extracted for combined JSON."))

            for index, row_data in sub_2A_filtered.iterrows():
                try:
                    # Check for existing entry before creating to avoid UniqueConstraint errors
                    # This assumes 'report_date' and 'state' together make a unique entry
                    obj, created = Table2AData.objects.update_or_create(
                        report_date=report_date, # Use the passed report_date
                        state=self._safe_string(row_data.get('state')),
                        defaults={
                            'thermal': self._safe_float(row_data.get('thermal')),
                            'hydro': self._safe_float(row_data.get('hydro')),
                            'gas_naptha_diesel': self._safe_float(row_data.get('gas_naptha_diesel')),
                            'solar': self._safe_float(row_data.get('solar')),
                            'wind': self._safe_float(row_data.get('wind')),
                            'other_biomass_co_gen_etc': self._safe_float(row_data.get('other_biomass_co_gen_etc')),
                            'total': self._safe_float(row_data.get('total')),
                            'drawal_sch': self._safe_float(row_data.get('drawal_sch')),
                            'act_drawal': self._safe_float(row_data.get('act_drawal')),
                            'ui': self._safe_float(row_data.get('ui')),
                            'requirement': self._safe_float(row_data.get('requirement')),
                            'shortage': self._safe_float(row_data.get('shortage')),
                            'consumption': self._safe_float(row_data.get('consumption')),
                        }
                    )
                    if created:
                        self.stdout.write(self.style.SUCCESS(f"➕ Created Table 2A entry for {report_date} - {row_data.get('state')}"))
                    else:
                        self.stdout.write(self.style.SUCCESS(f"🔄 Updated Table 2A entry for {report_date} - {row_data.get('state')}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ Error saving Table 2A row to DB (State: {row_data.get('state')}): {e}"))
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(A) data saved to database."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(A) not found or extraction failed."))

        # --- Extract Table 2(C) ---
        sub_2C = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r"2\s*\(C\)\s*State's\s*Demand\s*Met\s*in\s*MWs.*",
            end_marker=r"3\s*\(A\)\s*StateEntities\s*Generation:",
            header_row_count=2,
            debug_table_name="Table 2(C)"
        )
        if sub_2C is not None:
            column_mapping_2C = {
                'State': 'state',
                'Maximum Demand Met of the day': 'max_demand_met_of_the_day',
                'Time': 'time_max_demand_met',
                'Shortage during maximum demand': 'shortage_during_max_demand',
                'Requirement at maximum demand': 'requirement_at_max_demand',
                'Maximum requirement of the day': 'max_requirement_of_the_day',
                'Time.1': 'time_max_requirement',
                'Shortage during maximum requirement': 'shortage_during_max_requirement',
                'Demand Met at maximum Requirement': 'demand_met_at_max_requirement',
                'Min Demand Met': 'min_demand_met',
                'Time.2': 'time_min_demand_met',
                'ACE_MAX': 'ace_max',
                'ACE_MIN': 'time_ace_max', # This column in the raw df contains the time for ACE_MAX
                'Time.3': 'ace_min',      # This column in the raw df contains the numeric ACE_MIN
                'Time.4': 'time_ace_min' # This column in the raw df contains the time for ACE_MIN
            }

            sub_2C_renamed = sub_2C.rename(columns=column_mapping_2C)
            sub_2C_filtered = sub_2C_renamed[[col for col in column_mapping_2C.values() if col in sub_2C_renamed.columns]]

            # self.stdout.write(f"\n--- Debugging Table 2C Filtered Data ---")
            # self.stdout.write(f"Columns: {sub_2C_filtered.columns.tolist()}")
            # self.stdout.write(f"Sample data:\n{sub_2C_filtered.head().to_string()}")
            # self.stdout.write(f"----------------------------------------\n")

            combined_json_data['table_2C'] = sub_2C_filtered.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) extracted for combined JSON."))

            for index, row_data in sub_2C_filtered.iterrows():
                try:
                    # Check for existing entry before creating to avoid UniqueConstraint errors
                    obj, created = Table2CData.objects.update_or_create(
                        report_date=report_date, # Use the passed report_date
                        state=self._safe_string(row_data.get('state')),
                        defaults={
                            'max_demand_met_of_the_day': self._safe_float(row_data.get('max_demand_met_of_the_day')),
                            'time_max_demand_met': self._safe_string(row_data.get('time_max_demand_met')),
                            'shortage_during_max_demand': self._safe_float(row_data.get('shortage_during_max_demand')),
                            'requirement_at_max_demand': self._safe_float(row_data.get('requirement_at_max_demand')),
                            'max_requirement_of_the_day': self._safe_float(row_data.get('max_requirement_of_the_day')),
                            'time_max_requirement': self._safe_string(row_data.get('time_max_requirement')),
                            'shortage_during_max_requirement': self._safe_float(row_data.get('shortage_during_max_requirement')),
                            'demand_met_at_max_requirement': self._safe_float(row_data.get('demand_met_at_max_requirement')),
                            'min_demand_met': self._safe_float(row_data.get('min_demand_met')),
                            'time_min_demand_met': self._safe_string(row_data.get('time_min_demand_met')),
                            'ace_max': self._safe_float(row_data.get('ace_max')),
                            'ace_min': self._safe_float(row_data.get('ace_min')),
                            'time_ace_max': self._safe_string(row_data.get('time_ace_max')),
                            'time_ace_min': self._safe_string(row_data.get('time_ace_min')),
                        }
                    )
                    if created:
                        self.stdout.write(self.style.SUCCESS(f"➕ Created Table 2C entry for {report_date} - {row_data.get('state')}"))
                    else:
                        self.stdout.write(self.style.SUCCESS(f"🔄 Updated Table 2C entry for {report_date} - {row_data.get('state')}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ Error saving Table 2C row to DB (State: {self._safe_string(row_data.get('state'))}): {e}"))
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) data saved to database."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(C) not found or extraction failed."))

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
        today = datetime.date.today()
        today_str = today.strftime("%Y-%m-%d")

        # Check for existing report data using the new structure
        # We assume a report exists if any Table2AData entry for today exists
        if Table2AData.objects.filter(report_date=today).exists() or \
           Table2CData.objects.filter(report_date=today).exists():
            self.stdout.write(self.style.WARNING(f"⚠️ Report data for {today_str} already exists in the database. Skipping download and extraction."))
            return

        url = f"https://nrldc.in/get-documents-list/111?start_date={today_str}&end_date={today_str}"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://nrldc.in/reports/daily-psp",
        }

        self.stdout.write(f"🌐 Fetching NRDC report metadata for {today_str}...")
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
            self.stdout.write(self.style.WARNING(f"No report available for today ({today_str}). This might be due to weekends, holidays, or late publishing."))
            return

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

        # Removed the NRDCReport.objects.create line
        # Instead, pass the 'today' date directly to extract_tables_from_pdf
        self.extract_tables_from_pdf(pdf_path, output_dir, today) # Pass today's date