from .tasks import page_parse
import time
from celery.result import ResultBase
##########################################
##########################################
if __name__ == '__main__':
    with open('madeInChina_product_pages.txt') as f:
        urls = f.readlines()

    urls = [l.strip() for l in urls]

    total_urls = len(urls)
    print(total_urls)

    cnt_ = 0

    for url in urls:
        result = page_parse.delay(url)

        cnt_ = cnt_ + 1
        print('**********************************************')
        print(f'{cnt_} from {total_urls} has been done.')
        print('**********************************************')

