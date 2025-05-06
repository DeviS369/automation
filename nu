def compute():
    today = pd.to_datetime(datetime.today().date())
    data = {"IT_PULLOUT": []}
    supplyon, pullout = call_data(supplyon_bmw, data)
    supplyon['delivery_date'] = pd.to_datetime(supplyon['delivery_date'].replace('Backorder', pd.NaT), errors='coerce')
    supplyon = supplyon.dropna(subset=['delivery_date'])
    supplyon['delivery_quantity'] = pd.to_numeric(supplyon['delivery_quantity'], errors='coerce').fillna(0)
    pullout['con_date'] = pd.to_datetime(pullout['con_date'], errors='coerce')
    pullout['FKIMG'] = pd.to_numeric(pullout['FKIMG'], errors='coerce').fillna(0)
    pullout.loc[
        (pullout['con_date'].notna()) & (pullout['con_date'] < today) & (pullout['RECEP_FLG'] != 'X'),
        'RECEP_FLG'] = 'X'
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
        total_fkimg =  future_pullout['FKIMG'].sum()
        future_rows = group[group['delivery_date'] >= today].copy()
        future_rows['delivery_date'] = pd.to_datetime(future_rows['delivery_date'])
        future_rows = future_rows.sort_values('delivery_date')
        fkimg_counts = future_pullout['FKIMG'].value_counts()
        unique = future_pullout[future_pullout['FKIMG'].isin(fkimg_counts[fkimg_counts == 1].index)]
        fkimgs = unique['FKIMG'].tolist()
        future_pullout = future_pullout.drop_duplicates(subset=['buyer_article_no', 'con_date'], keep='first')
        con_dates = future_pullout['con_date'].tolist()
        future_rows = group[group['delivery_date'] >= today].copy()
        future_rows = future_rows.sort_values('delivery_date')
        previous_stock_val = stock_val
        row_index = 0
        con_added = False
        first_set = False
        for i, con_date in enumerate(con_dates):
            if i == 0:
                git_qty = int(total_fkimg)
            elif i <= len(fkimgs):
                fkimg_value = int(fkimgs[i - 1])
                git_qty = fkimg_value
            else:
                git_qty = 0
            next_git_qty = git_qty - fkimgs[i] if i < len(fkimgs) else git_qty 
            next_git_date = con_dates[i] if i < len(con_dates) else con_dates[i]
            while row_index < len(future_rows):
                row = future_rows.iloc[row_index]
                if row['delivery_date'] <= con_date:
                    rec = supplyon_bmw.objects.get(pk=row['id'])
                    if not first_set:
                        rec.blg_warehouse_stock = int(previous_stock_val) if int(previous_stock_val) > 0 else None
                        first_set = True
                    else:
                        if i < 0 and con_added:
                            previous_stock_val += next_git_qty
                            con_added = True
                        rec.blg_warehouse_stock = int(previous_stock_val) if int(previous_stock_val) > 0 else None
                    rec.git_qty = git_qty
                    rec.next_git_wh_qty = int(next_git_qty)
                    rec.next_git_wh_date = next_git_date
                    latest = bmw_warehouse.objects.filter(buyer_article_no=article).order_by('-entry_date').first()
                    rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0
                    previous_stock_val -= row['delivery_quantity']
                    update_records.append(rec)
                    row_index += 1
                else:
                    con_added = False
                    break
        while row_index < len(future_rows):
            row = future_rows.iloc[row_index]
            rec = supplyon_bmw.objects.get(pk=row['id'])
            rec.blg_warehouse_stock = int(previous_stock_val) if int(previous_stock_val) > 0 else 0
            rec.git_qty =  0
            rec.next_git_wh_qty = 0
            rec.next_git_wh_date = pd.NaT
            latest = bmw_warehouse.objects.filter(buyer_article_no=article).order_by('-entry_date').first()
            rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0
            previous_stock_val -= row['delivery_quantity']
            update_records.append(rec)
            row_index += 1
    if update_records:
        with transaction.atomic():
            supplyon_bmw.objects.bulk_update(
                update_records,
                ["blg_warehouse_stock", "git_qty", "next_git_wh_qty", "next_git_wh_date", "warehouse_stock"]
            )
        print(f"✅ Updated {len(update_records)} records successfully.")#compute()
compute()
