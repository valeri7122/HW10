from pymongo import MongoClient
import psycopg2
import json


# MongoDB parameters
USER = 'goitlearn'
PASS = 'vbv'
DB_NAME = 'HomeWork9Database1'
DOMAIN = 'cluster0.omrbmbh.mongodb.net'

# PosgresDB parameters
user = 'postgres'
dbname = 'postgres'
password = '123'


def from_mongo():
    try:
        client = MongoClient(f"mongodb+srv://{USER}:{PASS}@{DOMAIN}/{DB_NAME}?retryWrites=true&w=majority", ssl=True)
        db = client.HomeWork9Database1
        author_table = db.author.find()
        quote_table = db.quote.find()

        author_id_name_list = [] 
        author_list = []
        quote_list = []

        for author in author_table:
            author_id_name = {}
            id = author.pop('_id')
            author_id_name.update({id: author['fullname']})
            author_id_name_list.append(author_id_name)
            author_list.append(author)

        for quote in quote_table:
            quote.pop('_id')
            quote.pop('_cls')
            author_id = quote['author']
            for author in author_id_name_list:
                fullname = author.get(author_id)
                if fullname:
                    quote.update({'author': fullname})
            quote_list.append(quote)

        with open("data/authors.json", "w", encoding="utf-8") as fh:
            json.dump(author_list, fh, ensure_ascii=False, indent=2)
        with open("data/quotes.json", "w", encoding="utf-8") as fh:
            json.dump(quote_list, fh, ensure_ascii=False, indent=2)
    except:
        pass


def to_postgres():
    with psycopg2.connect(f"dbname={dbname} user={user} password={password}") as conn:
        with conn.cursor() as cur:

            query_sql = """ TRUNCATE TABLE quoteapp_author CASCADE """
            cur.execute(query_sql)
            query_sql = """ TRUNCATE TABLE quoteapp_tag CASCADE """
            cur.execute(query_sql)

            with open('data/authors.json', "rb") as fh:
                authors = json.load(fh)
                new_authors = []
                authors_list = []
                for author in authors:
                    author.update({"id": authors.index(author)+1})
                    new_authors.append(author)

                    if not author['fullname'] in authors_list:
                        authors_list.append(author['fullname'])

                query_sql_1 = """ insert into quoteapp_author
                    select * from json_populate_recordset(NULL::quoteapp_author, %s) """
                cur.execute(query_sql_1, (json.dumps(new_authors),))

            with open('data/quotes.json', "rb") as fh:
                quotes = json.load(fh)
                new_quotes = []
                for quote in quotes:
                    quote.update({"id": quotes.index(quote)+1})
                    if quote['author'] in authors_list:
                        quote.update({"author_id": authors_list.index(quote['author'])+1})
                        quote.pop('author')
                        new_quotes.append(quote)

                new_quote_tags = []
                new_tags = []
                tag_list = []
                i = 1
                j = 1
                for quote in quotes: 
                    for q in quote['tags']:
                        if not q in tag_list:
                            tag = {}
                            tag.update({"id": i})
                            tag.update({"name": q})
                            new_tags.append(tag)
                            tag_list.append(q)
                            i += 1

                        quote_tag = {}   
                        quote_tag.update({"id": j})
                        quote_tag.update({"quote_id": quotes.index(quote)+1})
                        quote_tag.update({"tag_id": tag_list.index(q)+1})
                        new_quote_tags.append(quote_tag)
                        j += 1
                        
                query_sql = """ insert into quoteapp_quote
                    select * from json_populate_recordset(NULL::quoteapp_quote, %s) """
                cur.execute(query_sql, (json.dumps(new_quotes),))
                query_sql = """ insert into quoteapp_tag
                    select * from json_populate_recordset(NULL::quoteapp_tag, %s) """
                cur.execute(query_sql, (json.dumps(new_tags),))
                query_sql = """ insert into quoteapp_quote_tags
                    select * from json_populate_recordset(NULL::quoteapp_quote_tags, %s) """
                cur.execute(query_sql, (json.dumps(new_quote_tags),))          


if __name__ == "__main__":
    from_mongo()
    to_postgres()
