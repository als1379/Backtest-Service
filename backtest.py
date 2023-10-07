import time
import concurrent.futures

from clickhouse_client import ClickhouseClient
from ranker import generate_random_ranker

class BackTest:
    
    def __init__(self) -> None:
        self.cl = ClickhouseClient()
        self.indices = self.get_indices()

    def release_client(self, client):
        self.available_clients.put(client)

    def get_indices(self):
        indices = {}
        cols = self.cl.execute_clickhouse_query("DESCRIBE ML.Impression_History")
        for index, col in enumerate(cols):
            indices[col[0]] = index

        return indices
    
    def new_get_indices(self):
        indices = {}
        cols = self.cl.execute_clickhouse_query("DESCRIBE BacktestImpressions")
        for index, col in enumerate(cols):
            indices[col[0]] = index

        return indices


    def get_search_ids_with_daterange(self, date_start, date_end):
        return self.cl.execute_clickhouse_query(
            f"SELECT search_id FROM (SELECT * FROM ML.Impression_History WHERE __time > '{date_start}' and __time < '{date_end}') GROUP BY search_id"
        )
    
    def new_get_search_ids_with_daterange(self, date_start, date_end):
        return self.cl.execute_clickhouse_query(
            f"SELECT * FROM BacktestImpressions WHERE __time > '{date_start}' and __time < '{date_end}'"
        )

    def get_results_with_search_id(self, search_id):
        return self.cl.execute_clickhouse_query(
            f"SELECT * FROM ML.Impression_History WHERE search_id = '{search_id[0]}'"
        )

    def rerank(self, rerank_scores, ac_list):
        print(ac_list)
        return sorted(
            ac_list, key=lambda x: rerank_scores[str(x[self.indices['place_code']])]
        )

    def claculate_avrage_click(self, search_id):
        start = time.time()
        page_list = self.get_results_with_search_id(search_id)
        end = time.time()
        print('get page list for a search id', end - start)

        start = time.time()
        rerank_page_list = self.rerank(self.rerank_scores, page_list)
        end = time.time()
        print('rerank', end - start)

        start = time.time()
        average_click_count = 0
        average_click_sum = 0
        for ac in rerank_page_list:
            if ac[self.indices["clicked"]] == 1:
                average_click_sum += ac[self.indices["plp_rank"]]
                average_click_count += 1
        end = time.time()
        print('calculate metrics', end - start)

        return average_click_sum, average_click_count

    def new_claculate_avrage_click(self, search_id_record):
        start = time.time()
        rerank_page_list = self.rerank(self.rerank_scores, search_id_record[self.indices['accs']]['accs'])
        end = time.time()
        print('rerank', end - start)

        start = time.time()
        average_click_count = 0
        average_click_sum = 0
        for ac in rerank_page_list:
            if ac['clicked'] == 1:
                average_click_sum += ac['plp_rank']
                average_click_count += 1
        end = time.time()
        print('calculate metrics', end - start)

        return average_click_sum, average_click_count


    def calculate_metric(self, rerank_scores, date_start, date_end):
        start = time.time()
        search_ids = self.get_search_ids_with_daterange(date_start, date_end)
        end = time.time()
        self.rerank_scores = rerank_scores
        print("get search_ids", end - start)

        results = list(map(self.claculate_avrage_click, search_ids))

        print("Program finished!")

        return results
    
    def new_calculate_metric(self, rerank_scores, date_start, date_end):
        start = time.time()
        search_ids = self.new_get_search_ids_with_daterange(date_start, date_end)
        end = time.time()
        self.rerank_scores = rerank_scores
        print("get search_ids", end - start)

        results = list(map(self.new_claculate_avrage_click, search_ids))

        print("Program finished!")

        return results


def main():
    bt = BackTest()

    start = time.time()
    data = bt.cl.execute_clickhouse_query(
        "SELECT DISTINCT place_code FROM ML.Impression_History place_code WHERE __time > '2023-08-26 00:00:00' and __time < '2023-08-27 00:00:00'"
    )
    end = time.time()

    print("get place_codes", end - start)

    start = time.time()
    places = generate_random_ranker(data)
    end = time.time()

    print("generate ranker", end - start)

    # start = time.time()
    # print(bt.new_calculate_metric(places, "2023-08-26 00:00:00", "2023-08-26 00:05:00"))
    # end = time.time()

    # print("calculate metric", end - start)

    start = time.time()
    print(bt.calculate_metric(places, "2023-08-26 00:00:00", "2023-08-26 00:05:00"))
    end = time.time()

    print("calculate metric", end - start)

if __name__ == "__main__":
    main()