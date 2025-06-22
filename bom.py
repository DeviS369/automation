import os
import time
import pandas as pd
import logging
from datetime import datetime , timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from selenium import webdriver

# Logger setup
logging.basicConfig(
    filename='sap_automation.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)
logger = logging.getLogger(__name__)

class ScenarioOne:
    def __init__(self, driver):
        self.driver = driver
        self.wait = WebDriverWait(self.driver, 60)
        self.data_by_plant = {}
        self.failed_records = []  # For error tracking
        self.zm_results = []   # zmpoval results
        self.me2m_results = {}      # ME2M results
        self.mm60_results = {}      # MM60 results
        self.z2price_results = {}   # Z2PRICE results
        self.zcur_result = None
        self.url = os.environ.get('url')
        self.portal_login = os.environ.get('login_user')
        self.portal_pwd = os.environ.get('login_pwd')
        self.t_code_yb = os.environ.get('t_code_yb')
        self.t_code_zm = os.environ.get('t_code_zm')
        self.t_code_me = os.environ.get('t_code_me')
        self.t_code_mm = os.environ.get('t_code_mm')
        self.t_code_v = os.environ.get('t_code_v')
        self.t_code_z2 = os.environ.get('t_code_z2')
        self.t_code_zc = os.environ.get('t_code_zc')  
    def capture_error(self, name):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"scenario_1/errors/{name}_{timestamp}.png"
        try:
            self.driver.save_screenshot(filename)
            logger.warning(f"📸 Screenshot saved: {filename}")
        except Exception as e:
            logger.error(f"Failed to capture screenshot: {e}")

    def pipeline(self):
        try:
            logger.info("🚀 Pipeline started")
            if not self.ybom_page():
                logger.error("❌ YBOM loading failed.")
                return
            self.is_ybom_in()
            self.zm_page()
            self.me2m_page()
            self.mm60_page()
            self.z2price_page()
            self.zcur()
            self.cal_cost()
            logger.info("✅ Pipeline completed successfully")
        except Exception as e:
            logger.exception(f"Pipeline error: {e}")
            self.capture_error("pipeline_error")

    def ybom_page(self):
        """Navigate to the t-code YBOM page"""
        try:
            ybom_textbox = self.wait.until(
                EC.visibility_of_element_located((By.ID, "ToolbarOkCode"))
            )
            ybom_textbox.send_keys(self.t_code_yb)
            entered_text = ybom_textbox.get_attribute("value")
            assert entered_text == self.t_code_yb, f"Expected '{self.t_code_yb}', but got '{entered_text}'"
            ybom_textbox.send_keys(Keys.ENTER)
            print(f"✅ Entered T-code: {entered_text}")
            return True
        except Exception as e:
            print(f"❌ Failed to enter YBOM T-code: {e}")
            return False
    def is_ybom_in(self):
        input_excel_path = r'scenario_1/input/US_supplied_codes.xlsx'
        try:
            df = pd.read_excel(input_excel_path, header=1)
            df['Plant'] = df['Plant'].astype(str).str.strip()
            df['SAP Material Code'] = df['SAP Material Code'].astype(str).str.strip()
            df = df[df['Plant'].isin(['1100', '2650'])]
        except Exception as e:
            print(f"❌ Error reading input Excel: {e}")
            return

        self.data_by_plant.clear()
        self.ybom_results.clear()

        for _, row in df.iterrows():
            plant = row['Plant']
            material = row['SAP Material Code']
            max_retries = 2
            success = False
            for attempt in range(max_retries + 1):
                try:
                    plant_field = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::1:34"]')))
                    plant_field.clear()
                    plant_field.send_keys(plant)

                    mat_field = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::2:34"]')))
                    mat_field.clear()
                    mat_field.send_keys(material)

                    current_month = datetime.now().strftime("%m.%Y")
                    month_field = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::3:34"]')))
                    month_field.clear()
                    month_field.send_keys(current_month)

                    exe_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="M0:50::btn[8]"]')))
                    exe_btn.click()
                    time.sleep(10)

                    try:
                        #file_btn = self.driver.find_element(By.XPATH, '//*[@id="M0:48::btn[45]"]')
                        #file_btn.click()
                        #print(f"✅ Data found for {plant}_{material}")

                        # Parse scrap table after button click
                        table_rows = self.driver.find_elements(By.XPATH, '//*[@id="M1:46:2::0:0-tbl"]/tbody/tr')  # NEED to check xpath BP
                        table_data = []
                        for tr in table_rows:
                            cols = tr.find_elements(By.TAG_NAME, "td")
                            row_data = [td.text.strip() for td in cols[:27]]
                            table_data.append(row_data)

                        if table_data:
                            row_subset = pd.DataFrame([table_data[-1]], columns=[f"col_{i+1}" for i in range(27)])
                            row_dict = row_subset.iloc[0].dropna().to_dict()
                            self.data_by_plant.setdefault(plant, []).append(row_dict)

                        success = True
                        break

                    except Exception:
                        print(f"⚠️ No data found for {plant}_{material} on attempt {attempt+1}")
                        if attempt == max_retries:
                            self.failed_records.append((plant, material, 'No Data Found'))
                        continue

                except Exception as e:
                    print(f"❌ Error processing {plant}_{material} on attempt {attempt+1}: {e}")
                    if attempt == max_retries:
                        self.failed_records.append((plant, material, str(e)))
                    time.sleep(3)
                    continue

    def zm_page(self):
        """
        Process ZMPOVAL data by iterating over plants and materials stored in self.data_by_plant,
        removing unwanted columns, filtering by 'Code Text' == 'input', and running SAP UI steps.
        """
        for plant, rows in self.data_by_plant.items():
            for row in rows:
                # Convert row dict back to DataFrame with single row
                df = pd.DataFrame([row])

                # Drop unwanted columns if present
                for col_to_drop in ['Activity Type', 'M/c Time/UOM']:
                    if col_to_drop in df.columns:
                        df = df.drop(columns=[col_to_drop])

                df = df.dropna()
                if 'Code Text' in df.columns and 'Material' in df.columns:
                    # Set plant and material for SAP UI input
                    self.plant_code = str(plant)
                    print(f"Processing plant: {self.plant_code}")

                    plant_zm = self.wait.until(EC.visibility_of_element_located((By.ID, "M0:46:::1:34")))
                    plant_zm.clear()
                    plant_zm.send_keys(self.plant_code)
                    plant_zm.send_keys(Keys.ENTER)

                    df['Code Text_clean'] = df['Code Text'].astype(str).str.strip().str.lower()
                    input_rows = df[df['Code Text_clean'] == 'input']

                    for material in input_rows['Material']:
                        self.mat_code = str(material)
                        print(f"Processing material: {self.mat_code}")

                        input_code = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::2:34"]')))
                        input_code.clear()
                        input_code.send_keys(self.mat_code)
                        input_code.send_keys(Keys.ENTER)
                        time.sleep(3)

                        now = datetime.now()
                        current_year = now.year
                        current_month = now.month

                        date_ranges = [
                            (f"01.{current_year}", f"{current_month:02}.{current_year}"),
                            (f"01.{current_year - 1}", f"12.{current_year - 1}"),
                            (f"01.{current_year - 2}", f"12.{current_year - 2}"),
                        ]

                        # Store results per plant and material
                        if plant not in self.zm_results:
                            self.zm_results[plant] = {}

                        if self.mat_code not in self.zm_results[plant]:
                            self.zm_results[plant][self.mat_code] = []

                        for from_date, to_date in date_ranges:
                            print(f"🔍 Trying date range: {from_date} to {to_date}")

                            # Clear and set From Date
                            from_date_field = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::11:34"]')))
                            from_date_field.clear()
                            from_date_field.send_keys(from_date)

                            # Clear and set To Date
                            to_date_field = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::11:59"]')))
                            to_date_field.clear()
                            to_date_field.send_keys(to_date)
                            to_date_field.send_keys(Keys.ENTER)
                            time.sleep(3)

                            try:
                                # Execute and navigate UI (assumed no file save)
                                self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="M0:50::btn[8]"]'))).click()
                                self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:48::btn[45]"]'))).click()
                                self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:46:2:1::2:0"]'))).click()
                                self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:50::btn[0]"]'))).click()
                                time.sleep(2)

                                # Scrape the displayed table:
                                table = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="M1:46:2:1::2:0"]')))
                                html = table.get_attribute("outerHTML")
                                df = pd.read_html(html)[0]

                                # Calculate zmpoval_val as row-wise division
                                if 'Total Value' in df.columns and 'Total Quantity' in df.columns:
                                    df['Total Value'] = pd.to_numeric(df['Total Value'].astype(str).str.replace(',', ''), errors='coerce')
                                    df['Total Quantity'] = pd.to_numeric(df['Total Quantity'].astype(str).str.replace(',', ''), errors='coerce')
                                    df['zmpoval_val'] = df['Total Value'] / df['Total Quantity']
                                else:
                                    df['zmpoval_val'] = None

                                # Store in dictionary
                                self.zm_results[plant][self.mat_code].append({
                                    "from_date": from_date,
                                    "to_date": to_date,
                                    "data": df
                                })

                                print(f"✅ Data collected for {plant} - {self.mat_code} from {from_date} to {to_date}")

                            except Exception as e:
                                print(f"❌ Error during ZMPOVAL processing for {plant} - {self.mat_code} at date range {from_date} to {to_date}: {e}")
                                self.failed_records.append((plant, self.mat_code, f"zm_page error: {e}"))

    def me2m_page(self):
        try:
            me2m_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            me2m_textbox.clear()
            me2m_textbox.send_keys(self.t_code_me)
            me2m_textbox.send_keys(Keys.ENTER)
            time.sleep(2)
            self.me2m_results = {}
            for plant, mat_dict_list in self.data_by_plant.items():
                for mat_dict in mat_dict_list:
                    try:
                        material = mat_dict.get('SAP Material Code')
                        if not material:
                            continue
                        self.plant_code = plant
                        self.mat_code_me = material
                        df = pd.DataFrame([mat_dict])
                        # Input section for SAP fields
                        me2m_matbox = self.wait.until(
                            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::0:34"]'))
                        )
                        me2m_matbox.send_keys(self.mat_code_me)
                        me2m_ptbox = self.wait.until(
                            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::1:34"]'))
                        )
                        self.plant = int(plant)
                        print(self.plant)
                        me2m_ptbox.send_keys(self.plant)
                        self.wait.until(EC.element_to_be_clickable((By.ID,'M0:50::btn[8]'))).click()
                        try:
                            table = self.wait.until(EC.visibility_of_element_located((
                                By.XPATH, '/html/body/table/tbody/tr/td/div/form/div/div[4]/div/div[1]/div/div/table'
                            )))
                            headers = [header.text.strip() for header in table.find_elements(By.TAG_NAME, 'th')]
                            clean_headers = [h for h in headers if h]
                            print(f"[Headers] {clean_headers}")
                            required_columns = ["Net Order Value", "Order Quantity"]
                            missing_cols = [col for col in required_columns if col not in clean_headers]
                            if missing_cols:
                                raise ValueError(f"Missing required columns: {missing_cols}")
                            table_data = table.find_elements(By.TAG_NAME, 'tbody')
                            filtered_data = []
                            for tbody in table_data:
                                rows = tbody.find_elements(By.TAG_NAME, 'tr')
                                for row in rows[1:]:
                                    cells = row.find_elements(By.TAG_NAME, 'td')
                                    if len(cells) < len(clean_headers):
                                        continue
                                    row_data = {}
                                    for col in required_columns:
                                        idx = clean_headers.index(col)
                                        if idx < len(cells):
                                            row_data[col] = cells[idx].text.strip()
                                        else:
                                            row_data[col] = ''
                                    filtered_data.append(row_data)
                            df_filtered = pd.DataFrame(filtered_data, columns=required_columns)
                            print("[Filtered DataFrame]")
                            print(df_filtered)
                            if df_filtered.empty:
                                raise ValueError("No valid rows with required data.")
                            df_filtered["Order Quantity"] = pd.to_numeric(df_filtered["Order Quantity"].str.replace(',', ''), errors='coerce')
                            df_filtered["Net Order Value"] = pd.to_numeric(df_filtered["Net Order Value"].str.replace(',', ''), errors='coerce')
                            df_filtered = df_filtered.dropna(subset=["Order Quantity", "Net Order Value"])
                            if df_filtered.empty:
                                raise ValueError("No valid numeric data for calculation.")
                            top_row = df_filtered.iloc[2]
                            self.me2m_price = top_row["Net Order Value"] / top_row["Order Quantity"]
                            print(f"[ME2M Price] {self.me2m_price}")
                            # Output section: Save as dictionary
                            if plant not in self.me2m_results:
                                self.me2m_results[plant] = {}
                            self.me2m_results[plant][material] = {
                                "me2m_price": self.me2m_price
                            }
                            print(f"✅ Stored ME2M result for: {plant}_{material}")
                        except Exception as e:
                            print(f"[Error] ME2M Price extraction failed: {e}")
                            continue
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)
        return None

    def mm60_page(self):
        try:
            mm60_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            mm60_textbox.clear()
            mm60_textbox.send_keys(self.t_code_mm)
            mm60_textbox.send_keys(Keys.ENTER)
            time.sleep(2)
            self.mm60_results = {}
            for plant, mat_dict_list in self.data_by_plant.items():
                for mat_dict in mat_dict_list:
                    try:
                        material = mat_dict.get('SAP Material Code')
                        if not material:
                            continue
                        self.plant_code = plant
                        self.mat_code_mm = material
                        # Input section
                        mm60_matbox = self.wait.until(
                            EC.visibility_of_element_located((By.ID, 'M0:46:::1:34')))
                        mm60_matbox.clear()
                        mm60_matbox.send_keys(self.mat_code_mm)
                        mm60_ptbox = self.wait.until(
                            EC.visibility_of_element_located((By.ID, 'M0:46:::2:34')))
                        mm60_ptbox.clear()
                        mm60_ptbox.send_keys(self.plant_code)
                        time.sleep(4)
                        self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]'))).click()
                        try:
                            mm60_price_element = self.wait.until(EC.visibility_of_element_located(
                                (By.XPATH, '//*[@id="grid#C117#1,7#if"]')))
                            mm60_price_text = mm60_price_element.text.strip()
                            if mm60_price_text:
                                self.mm60_price = float(mm60_price_text.replace(',', ''))
                                # Output section: Save as dictionary
                                if plant not in self.mm60_results:
                                    self.mm60_results[plant] = {}
                                self.mm60_results[plant][material] = {
                                    "mm60_price": self.mm60_price
                                }
                                print(f"✅ Stored MM60 result for: {plant}_{material} → {self.mm60_price}")
                        except Exception as e:
                            print(f"❌ Error extracting MM60 price for {plant}_{material}: {e}")
                            continue
                    except Exception as e:
                        print(f"❌ Processing error for plant/material: {e}")
                        continue
        except Exception as e:
            print(f"❌ MM60 page navigation error: {e}")
        return None
    time.sleep(2)
   
    def z2price_page(self):
        """Navigate to the Z2P t-code and extract the max price based on valid date range, storing it per plant/material."""
        try:
            z2p_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            z2p_textbox.clear()
            z2p_textbox.send_keys(self.t_code_z2)
            z2p_textbox.send_keys(Keys.ENTER)
            time.sleep(2)
            self.z2price_results = {}
            for plant, mat_dict_list in self.data_by_plant.items():
                for mat_dict in mat_dict_list:
                    try:
                        material = mat_dict.get('SAP Material Code')
                        if not material:
                            continue
                        self.plant_code = plant
                        self.mat_code_yb = material
                        z2p_plbox = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::1:34')))
                        z2p_plbox.clear()
                        z2p_plbox.send_keys(self.plant_code)
                        z2p_plbox.send_keys(Keys.RETURN)
                        time.sleep(2)
                        z2mat_box = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::4:34')))
                        z2mat_box.clear()
                        z2mat_box.send_keys(self.mat_code_yb)
                        z2mat_box.send_keys(Keys.RETURN)
                        time.sleep(2)
                        self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]'))).click()
                        outer_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#C120')))
                        headers = [header.text.strip() for header in outer_table.find_elements(By.TAG_NAME, 'th')]
                        clean_headers = [h for h in headers if h]
                        table_data = outer_table.find_elements(By.TAG_NAME, 'tbody')
                        filtered_data = []
                        for tbody in table_data:
                            rows = tbody.find_elements(By.TAG_NAME, 'tr')
                            for row in rows[1:]:
                                cells = row.find_elements(By.TAG_NAME, 'td')
                                if len(cells) < len(clean_headers):
                                    continue
                                row_data = {}
                                for col in clean_headers:
                                    idx = clean_headers.index(col)
                                    row_data[col] = cells[idx].text.strip() if idx < len(cells) else ''
                                filtered_data.append(row_data)
                        df = pd.DataFrame(filtered_data, columns=clean_headers)
                        if 'Price' in df.columns:
                            df['Price'] = pd.to_numeric(df['Price'].str.replace(',', ''), errors='coerce')
                        if 'Valid From' in df.columns:
                            df['Valid From'] = pd.to_datetime(df['Valid From'], format='%d.%m.%Y', errors='coerce')
                        if 'Valid To' in df.columns:
                            df['Valid To'] = pd.to_datetime(df['Valid To'], format='%d.%m.%Y', errors='coerce')
                        if 'Valid To' in df.columns and not df.empty:
                            df_sorted = df.sort_values(by='Valid To', ascending=False)
                            latest = df_sorted.iloc[0]
                            self.price = latest['Price']
                            # Store result in dictionary
                            if plant not in self.z2price_results:
                                self.z2price_results[plant] = {}
                            self.z2price_results[plant][material] = {
                                "z2_price": self.price
                            }
                            print(f"✅ Stored Z2 price for: {plant}_{material} → {self.price}")
                    except Exception as e:
                        print(f"❌ Error processing Z2 price for {plant}_{material}: {e}")
                        continue
        except Exception as e:
            print(f"[z2price_page] Navigation error: {e}")
        return None
    
    @staticmethod
    def get_month_start_end(year: int, month: int):
        """Finding start and end dates"""
        start_of_month = datetime(year, month, 1)
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        end_of_month = next_month - timedelta(days=1)
        return start_of_month, end_of_month

    def zcur(self):
        """Scrape SAP ZCUR data and store USD-INR rate"""
        try:
            z2p_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            z2p_textbox.clear()
            z2p_textbox.send_keys(self.t_code_zc)
            z2p_textbox.send_keys(Keys.ENTER)
            valid_from = self.wait.until(EC.presence_of_element_located((By.ID, 'M0:46:::0:34')))
            valid_from.clear()
            valid_from.send_keys(self.end)
            valid_from.send_keys(Keys.RETURN)
            time.sleep(2)
            valid_to = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::0:59')))
            valid_to.clear()
            valid_to.send_keys(self.start)
            valid_to.send_keys(Keys.RETURN)
            time.sleep(2)
            clk_btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]')))
            clk_btn.click()
            time.sleep(2)
            table_tr = self.wait.until(EC.visibility_of_element_located((By.ID, 'C120-content')))
            headers = [header.text.strip() for header in table_tr.find_elements(By.TAG_NAME, 'th')]
            clean_headers = [h for h in headers if h]
            required_columns = ["EXCHANGE RATE TYPE", "FROM CURRENCY", "TO CURRENCY", "DATE", "EXCHANGE-RATE"]
            missing_cols = [col for col in required_columns if col not in clean_headers]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            table_data = table_tr.find_elements(By.TAG_NAME, 'tbody')
            filtered_data = []
            for tbody in table_data:
                rows = tbody.find_elements(By.TAG_NAME, 'tr')
                for row in rows[2:]:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) < len(clean_headers):
                        continue
                    row_data = {}
                    for col in required_columns:
                        idx = clean_headers.index(col)
                        row_data[col] = cells[idx].text.strip() if idx < len(cells) else ''
                    filtered_data.append(row_data)
            df = pd.DataFrame(filtered_data, columns=required_columns)
            df = df.dropna(subset=["DATE", "FROM CURRENCY", "TO CURRENCY", "EXCHANGE-RATE"])
            df['DATE'] = pd.to_datetime(df['DATE'], format='%d.%m.%Y', errors='coerce')
            usd_inr_df = df[
                (df['FROM CURRENCY'] == 'USD') &
                (df['TO CURRENCY'] == 'INR')
            ]
            usd_inr_df = usd_inr_df.dropna(subset=['DATE'])
            usd_inr_df = usd_inr_df.sort_values(by='DATE', ascending=False)
            if not usd_inr_df.empty:
                latest = usd_inr_df.iloc[0]
                self.usd_price = latest['EXCHANGE-RATE']
                print(f"[usd-price] Latest USD to INR rate on {latest['DATE'].date()}: {self.usd_price}")
                return self.usd_price
        except Exception as e:
            print(f"[usd-price_page] Error: {e}")
            return None

    def cal_cost(self):
        """Calculate Finish Rate, Gross RM, Net RM and generate report per plant/material"""
        output_folder = r'E:\steel_automation\scenario_1\reports'
        os.makedirs(output_folder, exist_ok=True)
        us_cost = float(self.usd_price) if self.usd_price else None
        for plant, mat_list in self.data_by_plant.items():
            writer_path = os.path.join(output_folder, f'{plant}_report.xlsx')
            with pd.ExcelWriter(writer_path, engine='xlsxwriter') as writer:
                for mat_dict in mat_list:
                    material = mat_dict.get('SAP Material Code')
                    if not material:
                        continue
                    selected_price = None
                    if hasattr(self, 'zm_results') and self.zm_results.get(plant, {}).get(material):
                        selected_price = self.zm_results[plant][material]
                    elif hasattr(self, 'me2m_results') and self.me2m_results.get(plant, {}).get(material):
                        selected_price = self.me2m_results[plant][material]
                    elif hasattr(self, 'mm60_results') and self.mm60_results.get(plant, {}).get(material):
                        selected_price = self.mm60_results[plant][material]
                    if not selected_price:
                        print(f"[cal_cost] ❌ No valid ZM, ME2M or MM60 price found for {plant}_{material}. Skipping.")
                        continue
                    z2_price = self.z2price_results.get(plant, {}).get(material, {}).get('z2_price')
                    df = pd.DataFrame(mat_dict, index=[0])
                    df = df.iloc[:, :27]
                    df.columns = df.columns.str.strip()
                    df.columns = pd.io.common.dedup_names(list(df.columns), is_potential_multiindex=False)
                    if len(df.columns) > 19:
                        df = df.rename(columns={
                            df.columns[16]: 'RM-Weight',
                            df.columns[17]: 'Scrap1 wt',
                            df.columns[19]: 'Scrap3 wt'
                        })
                        df['RM-Weight'] = pd.to_numeric(df['RM-Weight'], errors='coerce')
                        df['Scrap1 wt'] = pd.to_numeric(df['Scrap1 wt'], errors='coerce')
                        df = df.dropna(subset=['Scrap1 wt'])
                        if not df['RM-Weight'].dropna().empty:
                            Rm_Weight = df['RM-Weight'].dropna().iloc[-1]
                            current = Rm_Weight
                            for i in reversed(df.index):
                                scrap = abs(df.at[i, 'Scrap1 wt'])
                                current -= scrap
                                df.at[i, 'Scrap3 wt'] = current
                            if 'Scrap Rate1' in df.columns:
                                scrap_rate_1 = pd.to_numeric(df['Scrap Rate1'], errors='coerce').dropna().iloc[-1]
                                df['scrap_cost_1'] = scrap_rate_1 * df['Scrap1 wt']
                            df['gross_rm'] = None
                            rm_weights = df['RM-Weight'].dropna().iloc[1:]
                            if not rm_weights.empty:
                                weight = rm_weights.iloc[-1]
                                df.at[df.index[-1], 'gross_rm'] = round(selected_price * weight, 2)
                            df['net_rm'] = None
                            start_index = df['gross_rm'].last_valid_index()
                            if start_index is not None:
                                net_value = df.at[start_index, 'gross_rm']
                                for i in reversed(df.index[:start_index + 1]):
                                    scrap = abs(df.at[i, 'scrap_cost_1']) if 'scrap_cost_1' in df.columns else 0
                                    if pd.notnull(scrap):
                                        net_value -= scrap
                                    df.at[i, 'net_rm'] = round(net_value, 2)
                            grand_total = {
                                'Scrap1 wt': df['Scrap1 wt'].sum(skipna=True),
                                'RM-Weight': df['RM-Weight'].dropna().iloc[-1] if not df['RM-Weight'].dropna().empty else None,
                                'gross_rm': df['gross_rm'].dropna().iloc[-1] if not df['gross_rm'].dropna().empty else None,
                                'scrap_cost_1': df['scrap_cost_1'].sum(skipna=True) if 'scrap_cost_1' in df.columns else None,
                                'net_rm': df['net_rm'].dropna().iloc[0] if not df['net_rm'].dropna().empty else None
                            }
                            net_rm_val = grand_total['net_rm'] if grand_total['net_rm'] else 0
                            new_row = pd.Series([None] * len(df.columns), index=df.columns)
                            new_row['Scrap1 wt'] = grand_total['Scrap1 wt']
                            new_row['RM-Weight'] = grand_total['RM-Weight']
                            new_row['gross_rm'] = grand_total['gross_rm']
                            new_row['scrap_cost_1'] = grand_total['scrap_cost_1']
                            new_row['net_rm'] = float(net_rm_val)
                            new_row['net_rm_usd'] = round(float(net_rm_val) / us_cost, 2) if us_cost else None
                            new_row['price_us'] = z2_price
                            new_row['price_in'] = round(new_row['net_rm_usd'] / z2_price, 2) if z2_price else None
                            new_row[df.columns[0]] = 'Grand Total'
                            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                            df.to_excel(writer, sheet_name=material, index=False)
                            worksheet = writer.sheets[material]
                            worksheet.write(df.shape[0], 0, '')  # Blank row between materials
            print(f'✅ Saved multi-sheet report for plant {plant}: {writer_path}')
if __name__ == '__main__':
    driver_instance = webdriver.Chrome()        
    try:
        obj = ScenarioOne(driver_instance)
        obj.check_get_url()
        obj.user_name()      
        obj.pass_word()
        obj.submit_page()
        obj.ybom_page()
        driver_instance.implicitly_wait(5)
        obj.back_btns()
        t1 = obj.me2m_page()
        if not t1:
            t2 = obj.mm60_page()
        obj.back_btns()
        obj.z2price_page()
        obj.call_back1()
        obj.zcur()
        obj.call_back1()
        obj.cal_cost()
        obj.move_files()
    except WebDriverException as e:
        logger.error("WebDriver exception: %s", e)
    finally:
        if driver_instance:
            driver_instance.quit()
