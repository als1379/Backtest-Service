from clickhouse_client import ClickhouseClient
# from .backtest import BackTest
import json

cl = ClickhouseClient()
# result = cl.execute_clickhouse_query("DESCRIBE ML.Impression_History")
# result = cl.client.command("""
#     CREATE TABLE IF NOT EXISTS BacktestImpressions (
#         search_id UUID,
#         __time DateTime64(3, 'UTC'),
#         page_number Nullable(Int32), 
#         app_id String,
#         ab_channel String,
#         plp String,
#         city String,
#         check_in_date Nullable(DateTime64(3, 'Asia/Tehran')),
#         check_out_date Nullable(DateTime64(3, 'Asia/Tehran')),
#         check_in_en String,
#         check_out_en String,
#         unquoted_page_urlquery String
#     ) Engine = MergeTree()
#     ORDER BY __time
# """)
# result = cl.execute_clickhouse_query("SHOW TABLES FROM ML")
# print(result)
# result = cl.execute_clickhouse_query("DESCRIBE BacktestImpressions")
# print(result)

constant_cols = ['search_id', '__time', 'page_number', 'app_id', 'ab_channel', 'plp', 'city', 'check_in_date', 'check_out_date', 'check_in_en', 'check_out_en', 'unquoted_page_urlquery']
accomidations_cols = ["place_code", "plp_rank", "total_plp_rank", "adjusted_plp_rank", "clicked", "maybe_ordered", "ordered", "order_id","total_price", "primary_price", "price", "rating", "rating_count", "place_type", "total_items"]

data_dict = {}

search_ids = cl.execute_clickhouse_query(
            f"SELECT search_id FROM (SELECT * FROM ML.Impression_History WHERE __time > '2023-08-26 00:00:00' and __time < '2023-08-26 00:05:00') GROUP BY search_id"
        )

def get_indices(cl):
    indices = {}
    cols = cl.execute_clickhouse_query("DESCRIBE ML.Impression_History")
    for index, col in enumerate(cols):
        indices[col[0]] = index

    return indices

indices = get_indices(cl)
for search_id in search_ids:
    print("**************")
    print(search_id)
    accs_with_search_id = cl.execute_clickhouse_query(
            f"SELECT * FROM ML.Impression_History WHERE search_id = '{search_id[0]}'"
        )
    accomodations = []
    for acc in accs_with_search_id:
        acc_obj = {}
        for acc_col in accomidations_cols:
            if acc_col == 'rating':
                try:
                    acc_obj[acc_col] = int(acc[indices[acc_col]])
                except TypeError:
                    acc_obj[acc_col] = None
            else:
                acc_obj[acc_col] = acc[indices[acc_col]]
        accomodations.append(acc_obj)
        
    for col in constant_cols:
        data_dict[col] = accs_with_search_id[0][indices[col]]

    accs = {
    "count" : len(accs_with_search_id),
    "accs": accomodations
    }
    print(accs)
    accs = json.dumps(accs)
    
    insert_data_query = f"""
        INSERT INTO BacktestImpressions (search_id, __time, page_number, app_id, ab_channel, plp, city, check_in_date, check_out_date, check_in_en, check_out_en, unquoted_page_urlquery, accs)
        VALUES (
            '{data_dict['search_id']}', 
            '{data_dict['__time']}',
            {data_dict['page_number']},
            '{data_dict['app_id']}',
            '{data_dict['ab_channel']}',
            '{data_dict['plp']}',
            '{data_dict['city']}',
            '{data_dict['check_in_date']}',
            '{data_dict['check_out_date']}',
            '{data_dict['check_in_en']}',
            '{data_dict['check_out_en']}',
            '{data_dict['unquoted_page_urlquery']}',
            '{accs}'
        )
    """

    print(insert_data_query)
    

    cl.get_client().command(insert_data_query)