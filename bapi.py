import re
from datetime import datetime, timedelta
from decimal import Decimal
from pyrfc import Connection
from decouple import config
import requests
import bmw_supplyon

def clean_numeric_value(value):
    """Cleans and converts numeric data."""
    if isinstance(value, (int, float, Decimal)): 
        return float(value)
    value = str(value).strip()
    value = re.sub(r'[^0-9.-]', '', value)
    try:
        return float(value)
    except ValueError:
        return 0

def portal_bapi():
    try:
        # Load configuration
        api_url = config("API_URL")
        bapi_name = config("BAPI")
        sold_from = config("CUS_CODE-SOLD_FROM")
        from_date = config("INV_DT-FROM_DATE")

        # Get access token
        response = requests.post(api_url, json={"apitype": "accesstoken"}, timeout=10)
        response.raise_for_status()
        token = response.json().get("token")
        if not token:
            raise ValueError("Token not found in the response")

        headers = {"Authorization": f"Bearer {token}"}

        # Authenticate
        auth_json = {"apitype": "login", "client": "RPS"}
        auth_response = requests.post(api_url, json=auth_json, headers=headers, timeout=10)
        auth_response.raise_for_status()
        auth_status = auth_response.json()

        # BAPI Connection
        conn_params = {
            "ashost": auth_status.get("ip"),
            "sysno": auth_status.get("sysnr"),
            "client": auth_status.get("client"),
            "user": auth_status.get("user_name"),
            "passwd": auth_status.get("password"),
        }
        conn_result = Connection(**conn_params)

        to_date = datetime.now().strftime("%Y%m%d")
        parameters = {"INV_DT": {"FROM_DATE": from_date, "TO_DATE": to_date}, "CUS_CODE": {"SOLD_FROM": sold_from}}
        result = conn_result.call(bapi_name, **parameters)

        # Process Data
        json_data = result
        df_pullout = json_data.get("PULLOUT", [])

        # Iterate through BMW Supplyon records
        bmw_supplyon_data = bmw_supplyon.objects.filter(
            buyer_id="BMW", year=year, created_at_week_no=week_no
        ).values("id", "buyer_article_no", "order_no", "delivery_quantity", "creation_date", "delivery_date")

        for row in bmw_supplyon_data:
            article_no = row["buyer_article_no"]

            # Fetch `delivery_date` from bmw_supplyon_data
            delivery_date = row.get("delivery_date")

            # Find `bal_pull` based on buyer_article_no and delivery_date
            bal_pull = next((clean_numeric_value(item["BAL_PULLQTY"]) for item in df_pullout if item.get("KDMAT") == article_no), 0)

            # Find `git_qty` where RECEP_FLG != 'X' and FMG == 0
            git_qty_l = next(
                (clean_numeric_value(item["PULL_QTY"]) for item in df_pullout if item.get("KDMAT") == article_no and 
                 item.get("RECEP_FLG") != "X" and clean_numeric_value(item.get("FKIMG")) == 0), 0)

            # Find `next_git_qty` for the next month
            next_git_qty = 0
            next_month_date = datetime.now() + timedelta(days=30)
            for item in df_pullout:
                if item.get("KDMAT") == article_no:
                    item_delivery_date = item.get("SDATE")
                    if item_delivery_date:
                        delivery_date_obj = datetime.strptime(item_delivery_date, "%Y-%m-%d")
                        if next_month_date.month == delivery_date_obj.month and next_month_date.year == delivery_date_obj.year:
                            next_git_qty = clean_numeric_value(item["PULL_QTY"])
                            break

            # if use delivery_date 
            from datetime import datetime, timedelta

            # Find `next_git_qty` for the next month
            next_git_qty = 0
            next_month_date = datetime.now() + timedelta(days=30)

            # Get `delivery_date` for the article_no from bmw_supplyon_data
            delivery_date_str = next((row["delivery_date"] for row in bmw_supplyon_data if row["buyer_article_no"] == article_no), None)

            if delivery_date_str:
                try:
                    delivery_date_obj = datetime.strptime(delivery_date_str, "%Y-%m-%d")  # Convert to datetime

                    for item in df_pullout:
                        if item.get("KDMAT") == article_no:
                            item_delivery_date = item.get("SDATE")  # Still using SDATE from pullout

                            if item_delivery_date:
                                item_delivery_date_obj = datetime.strptime(item_delivery_date, "%Y-%m-%d")

                                # Compare the delivery date from bmw_supplyon with pullout data
                                if next_month_date.month == item_delivery_date_obj.month and next_month_date.year == item_delivery_date_obj.year:
                                    next_git_qty = clean_numeric_value(item["P_QTY"])
                                    break

                except ValueError as e:
                    print(f"Error parsing delivery_date: {e}")



            # Fetch warehouse stock
            warehouse_stock_qs = bmw_warehouse.objects.filter(buyer_article_no=article_no).values("warehouse_qty")
            warehouse_stock_j = warehouse_stock_qs[0]["warehouse_qty"] if warehouse_stock_qs else 0
            warehouse_stock_j = clean_numeric_value(warehouse_stock_j)

            # Compute required quantities
            safety_stock_alm_o = int(warehouse_stock_j) - int(bal_pull) + int(git_qty_l)
            short_fall_dem_qty_p = int(clean_numeric_value(row.get("delivery_quantity", 0))) - int(bal_pull)
            tot_dem_qty_raise_q = safety_stock_alm_o + short_fall_dem_qty_p

            # Convert Message Date
            creation_date = row.get("creation_date", "").strip()
            creation_date = datetime.strptime(creation_date, "%Y-%m-%d") if creation_date else None

            if creation_date:
                dem_dt_prod_r = creation_date - timedelta(days=100)
                dem_sea_s = creation_date - timedelta(days=70)
                dem_air_t = creation_date - timedelta(days=25)
            else:
                dem_dt_prod_r = dem_sea_s = dem_air_t = None

            # Material Position Calculation
            if bal_pull != 0 and git_qty_l == 0 and next_git_qty == 0:
                mat_pos_u = "Sufficient stock available in warehouse against call-off"
            elif bal_pull == 0 and git_qty_l != 0 and next_git_qty != 0:
                mat_pos_u = "Alert on GIT material - Not reported on time against call-off"
            elif bal_pull == 0 and git_qty_l == 0 and next_git_qty == 0:
                mat_pos_u = "Stock not available - Plan for dispatch"
            else:
                mat_pos_u = "Unknown"

            # Find `ETA_DESTI` based on buyer_article_no (KDMAT in df_pullout)
            eta_desti = next((item["ETA_DESTI"] for item in df_pullout if item.get("KDMAT") == article_no), None)
            next_git_qty_date_n = datetime.strptime(eta_desti, "%Y-%m-%d").date() if eta_desti else None
        
            # Update Database Record
            update_records = []
            for row in bmw_supplyon_data:
                try:
                    record = bmw_supplyon.objects.get(pk=row["id"], year=year, created_at_week_no=week_no)
                    record.warehouse_stock = warehouse_stock_j
                    record.blg_warehouse_stock = bal_pull
                    record.git_qty = git_qty_l
                    record.next_git_wh_qty = next_git_qty
                    record.next_git_wh_date = next_git_qty_date_n
                    record.safety_stock_alarm = safety_stock_alm_o
                    record.short_fall_demand_qty = short_fall_dem_qty_p
                    record.tot_demand_qty_raise = tot_dem_qty_raise_q
                    record.demand_dt_prod = dem_dt_prod_r
                    record.dem_sea = dem_sea_s
                    record.dem_air = dem_air_t
                    record.mat_pos = mat_pos_u
                    update_records.append(record)
                except Exception as e:
                    print(f"Error updating record {row['id']}: {str(e)}")

            # Perform bulk update
            if update_records:
                bmw_supplyon.objects.bulk_update(
                    update_records,
                    [
                        "warehouse_stock", "blg_warehouse_stock", "git_qty", "next_git_wh_qty", "next_git_wh_date",
                        "safety_stock_alarm", "short_fall_demand_qty", "tot_demand_qty_raise", "demand_dt_prod",
                        "dem_sea", "dem_air", "mat_pos"
                    ]
                )

    except Exception as e:
        print(f"Error: {str(e)}")