from .tasks import page_parse
import time
from celery.result import ResultBase
##########################################
##########################################
if __name__ == '__main__':
    with open('all_category_pages_madeInChina-2.txt') as f:
        urls = f.readlines()

    urls = [l.strip() for l in urls]

    total_urls = len(urls)
    print(total_urls)

    # ff = open('all_categories_madeInChina-3.txt', 'w')

    cnt_ = 0

    for url in urls:
        result = product_parse.delay(url)
        print('Task finished?',result.ready())
        print('Task result:',result.result)

        #
        # time.sleep(4)
        #
        # print('Task finished?', result.ready())
        #
        # if (result.result):
        #     for l in result.result:
        #         ff.write(l + '\n')

        cnt_ = cnt_ + 1
        print('**********************************************')
        print(f'{cnt_} from {total_urls} has been done.')
        print('**********************************************')



    #ff.close()