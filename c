def compute_blg_stock_final():
    today = pd.to_datetime(datetime.today().date())
    data = {"IT_PULLOUT": []}
    supplyon, pullout = call_data(supplyon_bmw, data)
    supplyon.to_csv('02may_supplyon.csv')

    # Clean and prepare data
    supplyon['delivery_date'] = pd.to_datetime(
        supplyon['delivery_date'].replace('Backorder', pd.NaT), errors='coerce'
    )
    supplyon = supplyon.dropna(subset=['delivery_date'])
    supplyon['delivery_quantity'] = pd.to_numeric(supplyon['delivery_quantity'], errors='coerce').fillna(0)

    pullout['con_date'] = pd.to_datetime(pullout['con_date'], errors='coerce')
    pullout['FKIMG'] = pd.to_numeric(pullout['FKIMG'], errors='coerce').fillna(0)

    # Mark past con_dates as received
    pullout.loc[
        (pullout['con_date'].notna()) & (pullout['con_date'] < today) & (pullout['RECEP_FLG'] != 'X'),
        'RECEP_FLG'
    ] = 'X'

    # Sum received FKIMG by article
    blg_df = pullout[pullout['RECEP_FLG'] == 'X'].groupby('buyer_article_no', as_index=False)['FKIMG'].sum()
    blg_df = blg_df.rename(columns={'FKIMG': 'total_blg'})

    supplyon_sorted = supplyon.sort_values(['buyer_article_no', 'delivery_date'])
    update_records = []

    for article, group in supplyon_sorted.groupby('buyer_article_no', sort=False):
        group = group.sort_values('delivery_date')
        stock_row = blg_df.loc[blg_df['buyer_article_no'] == article, 'total_blg']
        stock_val = int(stock_row.iloc[0]) if not stock_row.empty else 0

        # Get future pullouts
        future_pullout = pullout[
            (pullout['buyer_article_no'] == article) &
            (pullout['RECEP_FLG'] != 'X') &
            (pullout['con_date'] > today)
        ].sort_values('con_date')

        # Drop duplicate con_dates with lower FKIMG
        future_pullout = future_pullout.sort_values('FKIMG', ascending=False)
        future_pullout = future_pullout.drop_duplicates(subset=['buyer_article_no', 'con_date'], keep='first')

        con_date_list = future_pullout['con_date'].dropna().sort_values().tolist()

        # Compute git/next_git per con_date
        con_date_git_map = {}
        for i, con_date in enumerate(con_date_list):
            current_fkimg = int(future_pullout[future_pullout['con_date'] >= con_date]['FKIMG'].sum())
            next_fkimg = int(future_pullout[future_pullout['con_date'] > con_date]['FKIMG'].sum())
            con_date_git_map[con_date] = {
                'git_qty': current_fkimg,
                'next_git_qty': current_fkimg - next_fkimg
            }

        # Append artificial max con_date for final window
        con_date_list.append(pd.Timestamp.max)

        # Future delivery rows
        future_rows = group[group['delivery_date'] >= today].copy()
        future_rows = future_rows.sort_values('delivery_date')
        row_index = 0
        previous_stock_val = stock_val

        for i in range(len(con_date_list) - 1):
            current_con_date = con_date_list[i]
            next_con_date = con_date_list[i + 1]
            git_info = con_date_git_map.get(current_con_date, {'git_qty': 0, 'next_git_qty': 0})

            while row_index < len(future_rows):
                row = future_rows.iloc[row_index]
                delivery_date = row['delivery_date']

                # Assign rows where delivery_date is <= current con_date
                if delivery_date <= current_con_date:
                    rec = supplyon_bmw.objects.get(pk=row['id'])
                    rec.blg_warehouse_stock = max(previous_stock_val, 0)
                    rec.git_qty = git_info['git_qty']
                    rec.next_git_wh_qty = git_info['next_git_qty']
                    rec.next_git_wh_date = current_con_date

                    latest = bmw_warehouse.objects.filter(
                        buyer_article_no=article
                    ).order_by('-entry_date').first()
                    rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0

                    update_records.append(rec)

                    previous_stock_val = int(previous_stock_val + git_info['next_git_qty'] - row['delivery_quantity'])
                    row_index += 1
                elif delivery_date < next_con_date:
                    # still in this con_date window, process it
                    rec = supplyon_bmw.objects.get(pk=row['id'])
                    rec.blg_warehouse_stock = max(previous_stock_val, 0)
                    rec.git_qty = git_info['git_qty']
                    rec.next_git_wh_qty = git_info['next_git_qty']
                    rec.next_git_wh_date = current_con_date

                    latest = bmw_warehouse.objects.filter(
                        buyer_article_no=article
                    ).order_by('-entry_date').first()
                    rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0

                    update_records.append(rec)

                    previous_stock_val = int(previous_stock_val + git_info['next_git_qty'] - row['delivery_quantity'])
                    row_index += 1
                else:
                    break

        # Handle any leftover delivery rows
        while row_index < len(future_rows):
            row = future_rows.iloc[row_index]
            rec = supplyon_bmw.objects.get(pk=row['id'])
            rec.blg_warehouse_stock = max(previous_stock_val, 0)
            rec.git_qty = 0
            rec.next_git_wh_qty = 0
            rec.next_git_wh_date = pd.NaT

            latest = bmw_warehouse.objects.filter(
                buyer_article_no=article
            ).order_by('-entry_date').first()
            rec.warehouse_stock = int(latest.warehouse_qty) if latest else 0

            update_records.append(rec)

            previous_stock_val = int(previous_stock_val - row['delivery_quantity'])
            row_index += 1

    # Commit bulk updates
    if update_records:
        with transaction.atomic():
            supplyon_bmw.objects.bulk_update(
                update_records,
                ["blg_warehouse_stock", "git_qty", "next_git_wh_qty", "next_git_wh_date", "warehouse_stock"]
            )
        print(f"✅ Updated {len(update_records)} records successfully.")
compute_blg_stock_final()
