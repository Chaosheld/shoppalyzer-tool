import sys
import pandas as pd
import query_index
import crawl_index
import notification
import traceback

#TODO: Dict externalisieren und Update periodisch automatisieren
def prepare_query(year_value):
    year_value = int(year_value)
    crawl_index_dict = {
        2019: ['2019-04', '2019-09', '2019-13', '2019-18', '2019-22', '2019-26', '2019-30', '2019-35', '2019-39',
               '2019-43', '2019-47', '2019-51'],
        2020: ['2020-05', '2020-10', '2020-16', '2020-24', '2020-29', '2020-34', '2020-40', '2020-45', '2020-50'],
        2021: ['2021-04', '2021-10', '2021-17', '2021-21', '2021-25', '2021-31', '2021-39', '2021-43', '2021-49'],
        2022: ['2022-05', '2022-21', '2022-27', '2022-33', '2022-40', '2022-49'],
        2023: ['2023-06', '2023-14', '2023-23', '2023-40', '2023-50'],
        2024: ['2024-10', '2024-18', '2024-22', '2024-26', '2024-30', '2024-33', '2024-38', '2024-42', '2024-46',
               '2024-51'],
        2025: ['2025-05']
    }
    if year_value in crawl_index_dict.keys():
        return crawl_index_dict[year_value]
    else:
        return []


if __name__ == "__main__":
    query_year = 2024
    if len(sys.argv) > 1:
        if sys.argv[1] == "query":
            """
                assumes a csv file called input.csv with domains row by row
                second argument optional year value with latest bucket as default
            """
            url_df = pd.read_csv("./files/input/input.csv", header=None)
            url_list = url_df[url_df.columns[0]].tolist()
            if len(url_list) > 0:
                index_list = []
                if len(sys.argv) == 3:
                    query_year = sys.argv[2]
                    index_list = prepare_query(query_year)
                if len(index_list) == 0:
                    index_list = prepare_query(query_year)
                # TODO: Chunking der Input-Liste in 1000er Blöcke
                query_index.query_athena(url_list, index_list)
            else:
                print("No domains provided. Please check input file.")
        elif sys.argv[1] == "crawl":
            """
            assumes a csv file called input.csv with domains row by row
            second argument optional year value with latest bucket as default
            """
            url_df = pd.read_csv("./files/input/input.csv", header=None)
            url_list = url_df[url_df.columns[0]].tolist()
            if len(url_list) > 0:
                index_list = []
                if len(sys.argv) == 3:
                    query_year = sys.argv[2]
                    index_list = prepare_query(query_year)
                if len(index_list) == 0:
                    index_list = prepare_query(query_year)
                # TODO: Limit dynamisch/regelbasiert und Live Crawl kombinieren
                try:
                    crawl_index.crawl_common_crawl(url_list, index_list, query_year, limit=500)
                    url_text = "\n".join(url_list)
                    subject = f'Shoppalyzer finished a task with {len(url_list)} URLs successfully!'
                    body = f"""The task for Shoppalyzer with {len(url_list)} URLs for {query_year} is done and  data is ready to be processed.
                    \nThe following URLs have been checked:
                    \n{url_text}
                    """
                    #notification.send_email(subject, body)
                except Exception as e:
                    error_message = traceback.format_exc()
                    subject = f'Shoppalyzer has encountered an issue that needs your attention'
                    body = f"""The task for Shoppalyzer has ended with an error:
                    
                    {traceback.format_exc()}
                    """
                    #notification.send_email(subject, body)
                    raise Exception

            else:
                print("No domains provided. Please check input file.")
        else:
            print("Please provide more detailed instructions.")