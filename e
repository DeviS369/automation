def compute_blg_stock():
    today = pd.to_datetime(datetime.today().date())
    data = {"IT_PULLOUT": []}
    supplyon, pullout = call_data(supplyon_bmw, data)
    supplyon['delivery_date'] = pd.to_datetime(supplyon['delivery_date'].replace('Backorder', pd.NaT), errors='coerce')
    supplyon = supplyon.dropna(subset=['delivery_date'])
    supplyon['delivery_quantity'] = pd.to_numeric(supplyon['delivery_quantity'], errors='coerce').fillna(0)
    pullout['con_date'] = pd.to_datetime(pullout['con_date'], errors='coerce')
    pullout['FKIMG'] = pd.to_numeric(pullout['FKIMG'], errors='coerce').fillna(0)
    pullout.loc[(pullout['con_date'].notna()) & (pullout['con_date'] < today) & (pullout['RECEP_FLG'] != 'X'), 'RECEP_FLG'] = 'X'
    blg_df = pullout[pullout['RECEP_FLG'] == 'X'].groupby('buyer_article_no', as_index=False)['FKIMG'].sum()
    blg_df = blg_df.rename(columns={'FKIMG': 'total_blg'})
    supplyon_sorted = supplyon.sort_values(['buyer_article_no', 'delivery_date'])
    update_records = []
    for article, group in supplyon_sorted.groupby('buyer_article_no', sort=False):
        group = group.sort_values('delivery_date')
        stock_row = blg_df.loc[blg_df['buyer_article_no'] == article, 'total_blg']
        stock_val = int(stock_row.iloc[0]) if not stock_row.empty else 0
        future_pullout = pullout[
            (pullout['buyer_article_no'] == article) &
            (pullout['RECEP_FLG'] != 'X') &
            (pullout['con_date'] > today)
        ].sort_values('con_date')
        fkimg_sum = future_pullout['FKIMG'].tolist()
        total_fkimg = int(sum(fkimg_sum))
        con_date_list = future_pullout['con_date'].tolist()
        future_pullout = future_pullout.sort_values('FKIMG', ascending=False)
        future_pullout = future_pullout.drop_duplicates(subset=['buyer_article_no', 'con_date'], keep='first')
        fkimg_list = future_pullout['FKIMG'].tolist()
        delivery_months = group[group['delivery_date'] >= today]['delivery_date'].dt.to_period('M').unique().tolist()
        month_git_qty = {}
        for i, month in enumerate(delivery_months):
            if i == 0:
                month_git_qty[month] = total_fkimg
            elif i < len(fkimg_list):
                month_git_qty[month] = int(fkimg_list[i - 1])
            else:
                month_git_qty[month] = 0
        month_next_git_qty = {}
        for i, month in enumerate(delivery_months):
            current_qty = month_git_qty[month]
            next_qty = month_git_qty[delivery_months[i + 1]] if i + 1 < len(delivery_months) else 0
            month_next_git_qty[month] = current_qty - next_qty
        previous_stock_val = stock_val
        for idx, row in group.iterrows():
            delivery_date = pd.to_datetime(row['delivery_date'], errors='coerce')
            if pd.isna(delivery_date) or delivery_date < today:
                continue
            delivery_date = pd.to_datetime(row['delivery_date'], errors='coerce')
            row_month = pd.Period(delivery_date, freq='M')
            git_qty = month_git_qty.get(row_month, 0)
            next_git_qty = month_next_git_qty.get(row_month, 0)
            next_git_date = pd.NaT
            for con_dt in con_date_list:
                if con_dt > delivery_date:
                    next_git_date = con_dt 
                    break
            supplyon.at[idx, 'blg_warehouse_stock'] =  previous_stock_val
            supplyon.at[idx, 'git_qty'] = git_qty
            supplyon.at[idx, 'next_git_wh_qty'] = next_git_qty
            supplyon.at[idx, 'next_git_wh_date'] =   next_git_date
            rec = supplyon_bmw.objects.get(pk=row['id'])
            rec.blg_warehouse_stock = previous_stock_val if previous_stock_val > 0 else 0
            rec.git_qty = git_qty
            rec.next_git_wh_qty = next_git_qty
 
            rec.next_git_wh_date =  next_git_date
            latest = bmw_warehouse.objects.filter(buyer_article_no=article).order_by('-entry_date').first()
            rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0
            update_records.append(rec)
            previous_stock_val = int(previous_stock_val + next_git_qty - row['delivery_quantity'])
    if update_records:
        #with transaction.atomic():
            supplyon_bmw.objects.bulk_update(
                update_records,
                ["blg_warehouse_stock", "git_qty", "next_git_wh_qty", "next_git_wh_date", "warehouse_stock"]
            )
            print(f"✅ Updated {len(update_records)} records successfully.")
compute_blg_stock()
