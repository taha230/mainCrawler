from .tasks import product_parse
import time
from celery.result import ResultBase
##########################################
##########################################
if __name__ == '__main__':
    with open('all_product_links.txt') as f:
        urls = f.readlines()

    urls = [l.strip() for l in urls]

    total_urls = len(urls)
    print(total_urls)

    cnt_ = 0

    for url in urls:
        result = product_parse.delay(url)
        cnt_ = cnt_ + 1
        print(f'{cnt_} from {total_urls} has been done.')