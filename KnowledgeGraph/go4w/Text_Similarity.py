import spacy
import time


#nlp = spacy.load("/tmp/la_vectorswiki-news-300d-1M-subword.vec_wiki_lg")
#nlp = spacy.load("/tmp/wiki-news-300d-1M.vec_wiki_lg")
nlp = spacy.load("crawl-300d-2M.vec_wiki_lg")

start = time.process_time()

doc1 = nlp("address")
doc2 = nlp("street")
print(doc1.similarity(doc2))


print("time : " + str(time.process_time() - start))
