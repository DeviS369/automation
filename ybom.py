"""Automates SAP YBOM,  data export using Selenium."""

import time
import os
import sys
import shutil
import pandas as pd
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load and validate environment variables
load_dotenv()
required_vars = [
    'url', 'login_user', 'login_pwd',
    't_code_yb', 't_code_zm', 't_code_me', 't_code_mm','t_code_v','t_code_z2','mat_code_yb','plant_code','mat_code_zmm'
]
missing_vars = [var for var in required_vars if not os.environ.get(var)]
if missing_vars:
    logger.error("Missing environment variables: %s", ', '.join(missing_vars))
    sys.exit(1)

# Assign variables
url = os.environ.get('url')
portal_login = os.environ.get('login_user')
portal_pwd = os.environ.get('login_pwd')
t_code_yb = os.environ.get('t_code_yb')
t_code_zm = os.environ.get('t_code_zm')
t_code_me = os.environ.get('t_code_me')
t_code_mm = os.environ.get('t_code_mm')
t_code_v = os.environ.get('t_code_v')
t_code_z2 = os.environ.get('t_code_z2')
mat_code_yb = os.environ.get('mat_code_yb')
plant_code = os.environ.get('plant_code')
mat_code_zmm = os.environ.get('mat_code_zmm')
t_code_zc = os.environ.get('t_code_zc')
# Get date info
year, week, weekday = datetime.now().isocalendar()
day = (week - 1) * 7 + weekday

# Chrome options
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument('--ignore-ssl-errors')
prefs = {
    "profile.default_content_settings.popups": 0,
    "directory_upgrade": True,
    "download.default_directory": r"E:\steel_automation\scenario_1\ybom_files",
    "download.prompt_for_download": False
}
chrome_options.add_experimental_option("prefs", prefs)

driver_instance = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)
wait = WebDriverWait(driver_instance, 40)

class ScenarioOne:
    """Class for SAP YBOM data scraping."""

    def __init__(self, web_driver):
        """Initialize the ScenarioOne object with a web driver."""
        self.driver = web_driver
        self.wait = wait
        self.start, self.end = self.get_month_start_end(2024, 3)
        self.start = self.start.strftime('%d.%m.%Y')
        self.end = self.end.strftime('%d.%m.%Y')

    def check_get_url(self):
        """Open the SAP portal using the provided URL."""
        try:
            self.driver.get(url)
        except WebDriverException as e:
            logger.error("Error opening URL: %s", e)
            self.driver.quit()
            sys.exit()

    def user_name(self):
        """Enter the username into the login form."""
        try:
            uname_field = self.wait.until(
                EC.presence_of_element_located((By.ID, 'sap-user'))
            )
            uname_field.send_keys(portal_login)
        except TimeoutException as e:
            logger.error("Timeout entering username: %s", e)

    def pass_word(self):
        """Enter the password into the login form."""
        try:
            pwd_field = self.wait.until(
                EC.presence_of_element_located((By.ID, 'sap-password'))
            )
            pwd_field.send_keys(portal_pwd)
        except TimeoutException as e:
            logger.error("Timeout entering password: %s", e)

    def submit_page(self):
        """Click the login button to submit the login form."""
        try:
            login_btn = self.wait.until(
                EC.presence_of_element_located((By.ID, 'LOGON_BUTTON'))
            )
            login_btn.click()
        except TimeoutException as e:
            logger.error("Timeout clicking login: %s", e)

    def is_logged_in(self, homepage_title: str) -> bool:
        """Check whether the login was successful based on the homepage title."""
        return (
            homepage_title.lower() in self.driver.current_url.lower()
            or homepage_title.lower() in self.driver.title.lower()
        )
    # Tcode YBOM
    def ybom_page(self):
        """Navigate to the t-code YBOM page"""
        ybom_textbox = self.wait.until(
            EC.visibility_of_element_located((By.ID, "ToolbarOkCode"))
        )
        ybom_textbox.send_keys(t_code_yb)
        ybom_textbox.send_keys(Keys.ENTER)

        plant_textbox = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::1:34"]'))
        )
        plant_textbox.send_keys(plant_code)
        
        material_code = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::2:34"]'))
        )
        material_code.send_keys(mat_code_yb)
        
        pro_month = self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::3:34"]'))
        )
        current_month = datetime.now().strftime("%m.%Y")
        pro_month.send_keys(current_month)

        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:50::btn[8]"]'))
        ).click()

        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:48::btn[45]"]'))
        ).click()

        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:46:2:1::2:0"]'))
        ).click()

        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:50::btn[0]"]'))
        ).click()

        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:48::btn[20]"]'))
        ).click()
        self.wait.until(
            EC.visibility_of_element_located((By.ID, 'UpDownDialogChoose'))
        ).click()
        time.sleep(10)
    # T-code ZMPOVAL_N
    time.sleep(2)
    def back_btns(self):
        """Navigate back from the data view to the main SAP screen."""
        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:56::btn[3]"]'))
        ).click()
        time.sleep(1)
        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:55::btn[15]"]'))
        ).click()
        time.sleep(4)
    def zm_page(self):
        """ Extract data using T-code ZMPOVAL_N """
        zm_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
        zm_textbox.send_keys(t_code_zm)
        zm_textbox.send_keys(Keys.ENTER)
        time.sleep(3)
        plant_zm = self.wait.until(EC.visibility_of_element_located((By.ID,"M0:46:::1:34")))
        plant_zm.send_keys(plant_code)
        plant_zm.send_keys(Keys.ENTER)
        time.sleep(2)
        material_code = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::2:34"]')))
        material_code.send_keys(mat_code_zmm)
        material_code.send_keys(Keys.ENTER)
        time.sleep(1)
    def generate_and_try_date_ranges(self):
        now = datetime.now()
        current_year = now.year
        current_month = now.month
        date_ranges = [
            (f"01.{current_year}", f"{current_month:02}.{current_year}"),
            (f"01.{current_year - 1}", f"12.{current_year - 1}"),
            ("01.2023", "12.2023"),
        ]
        time.sleep(5)
        for from_date, to_date in date_ranges:
            print(f"🔍 Trying date range: {from_date} to {to_date}")
            try:
                from_input = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::11:34"]')))
                from_input.clear()
                from_input.send_keys(from_date)
                time.sleep(2)
                to_input = self.wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::11:59"]')))
                to_input.clear()
                to_input.send_keys(to_date)
                to_input.send_keys(Keys.ENTER)
                time.sleep(2)
                try:
                    self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="M0:50::btn[8]"]'))).click()
                except TimeoutException:
                    print("⚠️ Execute button not found.")
                    continue
                time.sleep(5)
                if self.data_is_available():
                    print(f"✅ Data found for range: {from_date} to {to_date}")
                    return True
                else:
                    print("❌ No data found, clicking fallback buttons...")
                    try:
                        self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="SAPMSDYP10_1-close"]'))).click()
                        time.sleep(3)
                        self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="M0:56::btn[3]"]'))).click()
                    except TimeoutException:
                        print("⚠️ One or both fallback buttons not clickable.")
            except TimeoutException:
                print(f"⚠️ Timeout trying range: {from_date} to {to_date}")
                continue
        return False

    def data_is_available(self):
        try:
            table_div = self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="cnt2"]')))
            try:
                table = table_div.find_element(By.TAG_NAME,"table")
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) >= 2:
                    second_row = rows[1]
                    cells = second_row.find_elements(By.TAG_NAME, "td")
                    row_data = [cell.get_attribute("textContent").strip() for cell in cells]
                    if any(row_data):
                        df = pd.DataFrame([row_data])
                        print(df)
                        df.to_excel('zmpoval_files/zm.xlsx', index=False)
                        return True
                    else:
                        print("Second row is empty.")
                else:
                    print("Less than 2 rows in table.")
            except Exception as e:
                print(f"Error parsing table: {e}")
                return False
            return False
        except TimeoutException:
            print("Timeout: Could not find table div with ID 'cnt2'.")
            return False

    # T-code ME2M
    def me2m_page(self):
        """Navigate to the pass t-code me2m."""
        try:
            me2m_textbox = self.wait.until(
                    EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            me2m_textbox.send_keys(t_code_me)
            me2m_textbox.send_keys(Keys.ENTER)
            me2m_matbox = self.wait.until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::0:34"]'))
            )
            me2m_matbox.send_keys(mat_code_zmm)
            me2m_ptbox = self.wait.until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:46:::1:34"]')) 
            )
            me2m_ptbox.send_keys(plant_code)
            self.wait.until(EC.element_to_be_clickable((By.ID,'M0:50::btn[8]'))).click()
        except Exception as e:
            print(e)
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
            return self.me2m_price
        except Exception as e:
            print(f"[Error] ME2M Price extraction failed: {e}")
            return None
    time.sleep(2) 
    def call_back1(self):
        self.back_btns()

    #T-code MM6
    def mm60_page(self):
        """Pass tcode mm60 """
        mm60_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
        mm60_textbox.send_keys(t_code_mm)
        mm60_textbox.send_keys(Keys.ENTER)
        mm60_matbox = self.wait.until(
            EC.visibility_of_element_located((By.ID, 'M0:46:::1:34')))
        mm60_matbox.send_keys(mat_code_zmm)
        mm60_ptbox = self.wait.until(
            EC.visibility_of_element_located((By.ID, 'M0:46:::2:34')))
        mm60_ptbox.send_keys(plant_code)
        time.sleep(4)
        self.wait.until(
            EC.visibility_of_element_located((By.ID, 'M0:50::btn[8]'))).click()
        try:
            self.mm60_price =self.wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="grid#C117#1,7#if"]')))
            self.mm60_price = self.mm60_price.text
            return self.mm60_price 
        except Exception as e:
            print(f"Error parsing table div: {e}")
            return None
    time.sleep(2)
    # T-code Z2PRICE
    def z2price_page(self):
        """Navigate to the Z2P t-code and extract the max price based on valid date range"""
        try:
            z2p_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
            z2p_textbox.send_keys(t_code_z2)
            z2p_textbox.send_keys(Keys.ENTER)
            time.sleep(2)
            z2p_plbox = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::1:34')))
            z2p_plbox.send_keys(plant_code)
            z2p_plbox.send_keys(Keys.RETURN)
            time.sleep(2)
            z2mat_box = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::4:34')))
            z2mat_box.send_keys(mat_code_yb)
            z2mat_box.send_keys(Keys.RETURN)
            time.sleep(2)
            self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]'))).click()
        except Exception as e:
            print(f"[z2price_page] Error: {e}")
            return None
        try:
            outer_table = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#C120')))
            headers = [header.text.strip() for header in outer_table.find_elements(By.TAG_NAME, 'th')]
            clean_headers = [h for h in headers if h]
            print(f"[Headers] {clean_headers}")
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
                        if idx < len(cells):
                            row_data[col] = cells[idx].text.strip()
                        else:
                            row_data[col] = ''
                    filtered_data.append(row_data)
            df = pd.DataFrame(filtered_data, columns=clean_headers)
            print("[Filtered DataFrame]")
            print(df)
            # Clean columns (only if they exist)
            if 'Price' in df.columns:
                df['Price'] = pd.to_numeric(df['Price'].str.replace(',', ''), errors='coerce')
            if 'Valid From' in df.columns:
                df['Valid From'] = pd.to_datetime(df['Valid From'], format='%d.%m.%Y', errors='coerce')
            if 'Valid To' in df.columns:
                df['Valid To'] = pd.to_datetime(df['Valid To'], format='%d.%m.%Y', errors='coerce')
            if 'Valid To' in df.columns:
                df_sorted = df.sort_values(by='Valid To', ascending=False)
                latest = df_sorted.iloc[0]
                self.price = latest['Price']
                print("z2price",self.price)
                return self.price
            else:
                raise Exception("'Valid To' column missing")
        except Exception as e:
            print(f"[z2price_page] Error: {e}")
            return None
    time.sleep(2)
    def z2price_page1(self):
        # //*[@id="grid#C120#8,7#if"]
        z2p_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
        z2p_textbox.send_keys(t_code_z2)
        z2p_textbox.send_keys(Keys.ENTER)
        self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::1:34'))).send_keys(plant_code, Keys.RETURN)
        time.sleep(1)
        self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::4:34'))).send_keys(mat_code_yb, Keys.RETURN)
        self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]'))).click()
        # table cnt57_row2

        """
        price = self.wait.until(EC.visibility_of_element_located((By.XPATH,'//*[@id="grid#C120#8,7#if"]'))).text
        print(price)
        """
    
    def log_out(self):
        """Log out from the SAP session."""
        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M0:55::btn[15]"]'))
        ).click()
        self.wait.until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="M1:46:::3:6"]'))
        ).click()
    
    @staticmethod
    def get_month_start_end(year: int, month: int):
        start_of_month = datetime(year, month, 1)
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        end_of_month = next_month - timedelta(days=1)
        return start_of_month, end_of_month
    
    def zcur(self):
        z2p_textbox = self.wait.until(EC.visibility_of_element_located((By.ID, "ToolbarOkCode")))
        z2p_textbox.send_keys(t_code_zc)
        z2p_textbox.send_keys(Keys.ENTER)
        valid_from = self.wait.until(EC.presence_of_element_located((By.ID, 'M0:46:::0:34')))
        valid_from.send_keys(self.end)
        valid_from.send_keys(Keys.RETURN)
        time.sleep(2)
        valid_to = self.wait.until(EC.visibility_of_element_located((By.ID, 'M0:46:::0:59')))
        valid_to.send_keys(self.start)
        valid_to.send_keys(Keys.RETURN)
        time.sleep(2)
        clk_btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'M0:50::btn[8]')))
        clk_btn.click()
        time.sleep(2)
        try:
            table_tr = self.wait.until(EC.visibility_of_element_located((By.ID,'C120-content')))
            headers = [header.text.strip() for header in table_tr.find_elements(By.TAG_NAME, 'th')]
            clean_headers = [h for h in headers if h]
            print(f"[Headers] {clean_headers}")
            required_columns = ["EXCHANGE RATE TYPE", "FROM CURRENCY","TO CURRENCY","DATE","EXCHANGE-RATE"]
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
                        if idx < len(cells):
                            row_data[col] = cells[idx].text.strip()
                        else:
                            row_data[col] = ''
                    filtered_data.append(row_data)
            df = pd.DataFrame(filtered_data, columns=required_columns)
            print("[Filtered DataFrame]")
            required_fields = ['DATE', 'FROM CURRENCY', 'TO CURRENCY', 'EXCHANGE-RATE']
            df = df.dropna(subset=required_fields)
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
            else:
                print("[usd-price] No valid USD to INR exchange rate data found.")
        except Exception as e:
            print(f"[[usd-price_page] Error: {e}")
            return None
        
    def cal_cost(self):
        """Calculate Finish Rate, Gross RM, Net RM and generate report"""
        folder_path = r'E:\steel_automation\scenario_1\ybom_files'
        output_folder = r'E:\steel_automation\scenario_1\reports'
        us_cost = float(self.usd_price) if self.usd_price else None
        selected_price = self.me2m_price if self.me2m_price else self.mm60_price
        if not selected_price:
            print("[cal_cost] ❌ No valid ME2M or MM60 price found. Skipping calculation.")
            return
        # Get Z2 price
        z2_price = self.price
        if not z2_price:
            print("[cal_cost] ⚠️ No Z2Price value found. Continuing with cost-only calculation.")
        os.makedirs(output_folder, exist_ok=True)
        files = os.listdir(folder_path)

        for file in files:
            if file.endswith('.xlsx'):
                file_path = os.path.join(folder_path, file)
                df = pd.read_excel(file_path, skiprows=9)
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
                        output_path = os.path.join(output_folder, f'res_{file}')
                        df.to_excel(output_path, index=False)
                        print(f'✅ Processed and saved: {output_path}')
    
    
    def move_files(self):
            source_folder1 = r'E:\steel_automation\scenario_1\ybom_files'
            source_folder2 = r'E:\steel_automation\scenario_1\reports'
            destination_folder = r'E:\steel_automation\archived'
            now = datetime.now()
            day_no = now.strftime("%j") 
            week_no = now.strftime("%U") 
            year = now.strftime("%Y")
            new_folder_name = f"day_{day_no}_week_{week_no}_year_{year}"
            new_dest_folder = os.path.join(destination_folder, new_folder_name)
            if not os.path.exists(new_dest_folder):
                os.makedirs(new_dest_folder)
            for filename in os.listdir(source_folder1):
                src_path = os.path.join(source_folder1, filename)
                if os.path.isfile(src_path):
                    shutil.move(src_path, os.path.join(new_dest_folder, filename))
                    print(f"Moved: {src_path} to {new_dest_folder}")
            for filename in os.listdir(source_folder2):
                src_path = os.path.join(source_folder2, filename)
                if os.path.isfile(src_path):
                    shutil.move(src_path, os.path.join(new_dest_folder, filename))
                    print(f"Moved: {src_path} to {new_dest_folder}")

if __name__ == '__main__':
    try:
        obj = ScenarioOne(driver_instance)
        obj.check_get_url()
        obj.user_name()
        obj.pass_word()
        obj.submit_page()
        obj.ybom_page()
        driver_instance.implicitly_wait(5)
        obj.back_btns()
        #driver_instance.implicitly_wait(5)
        #obj.zm_page()
        #t = obj.generate_and_try_date_ranges()
        #obj.call_back()
        #if not t :
        t1 = obj.me2m_page()
        #obj.call_back1()
        if not t1:
            t2 = obj.mm60_page()
        #obj.v_ld_page()
        obj.back_btns()
        obj.z2price_page()
        obj.call_back1()
        obj.zcur()
        obj.call_back1()
        obj.cal_cost()
        #obj.log_out()
        obj.move_files()
    except WebDriverException as e:
        logger.error("WebDriver exception: %s", e)
    finally:
        if driver_instance:
            driver_instance.quit()
