from grakn.client import GraknClient
import ijson

def build_mineral_schema_graph(inputs):
    with GraknClient(uri="localhost:48555") as client:
        with client.session(keyspace = "mineral") as session:
            for input in inputs:
                print("Loading from [" + input["data_path"] + "] into Grakn ...")
                load_data_into_grakn(input, session)

def load_data_into_grakn(input, session):
    items = parse_data_to_dictionaries(input)

    for item in items:
        with session.transaction().write() as transaction:
            graql_insert_query = input["template"](item)
            print("Executing Graql Query: " + graql_insert_query)
            transaction.query(graql_insert_query)
            transaction.commit()

    print("\nInserted " + str(len(items)) + " items from [ " + input["data_path"] + "] into Grakn.\n")

def company_template(company):
    graql_insert_query = 'insert $company isa company, has name "' + company["name"] + '"'
    graql_insert_query += ', has email "' + company["email"] + '"'
    graql_insert_query += ', has phone "' + company["phone"] + '"'
    graql_insert_query += ', has location "' + company["location"] + '"'
    graql_insert_query += ', has scope "' + company["scope"] + '"'
    graql_insert_query += ', has SN_ID "' + company["SN_ID"]+ '"'
    graql_insert_query += ', has company_id ' + str(company["company_id"])
    graql_insert_query += ";"
    return graql_insert_query

def person_template(person):
    # insert person
    graql_insert_query = 'insert $person isa person, has phone "' + person["phone"] + '"'

    graql_insert_query += ', has first_name "' + person["first_name"] + '"'
    graql_insert_query += ', has last_name "' + person["last_name"] + '"'
    graql_insert_query += ', has email "' + person["email"] + '"'
    graql_insert_query += ', has gender "' + person["gender"] + '"'
    graql_insert_query += ', has location "' + person["location"] + '"'
    graql_insert_query += ', has SN_ID "' + person["SN_ID"]+ '"'
    graql_insert_query += ', has person_id ' + str(person["person_id"])
    graql_insert_query += ";"
    return graql_insert_query

def website_template(website):
    # insert website
    graql_insert_query = 'insert $website isa website, has url "' + website["url"] + '"'

    graql_insert_query += ', has server "' + website["server"] + '"'
    graql_insert_query += ', has host "' + website["host"] + '"'
    graql_insert_query += ";"
    return graql_insert_query

def tag_template(tag):
    # insert tag
    graql_insert_query = 'insert $tag isa tag, has name "' + tag["name"] + '"'

    graql_insert_query += ', has score ' + str(tag["score"]) + ''
    graql_insert_query += ";"
    return graql_insert_query

def product_template(product):
    # insert product
    graql_insert_query = "insert $product isa product, has product_id " + str(product["product_id"])

    graql_insert_query += ', has name "' + product["name"] + '"'
    graql_insert_query += ";"
    return graql_insert_query

def employ_template(employ):
    # match company
    graql_insert_query = 'match $company isa company, has company_id ' + str(employ["company_id"]) + ';'
    # match person
    graql_insert_query += ' $employee isa person, has person_id ' + str(employ["person_id"]) + ';'
    # insert employ
    graql_insert_query += ' insert (employer: $company, employee: $employee) isa employ, has position "' + employ["position"] + '"'
    graql_insert_query += ";"
    return graql_insert_query

def broker_relation_template(broker_relation):
    # match buyer
    graql_insert_query = 'match $buyer isa person, has person_id ' + str(broker_relation["broker_buyer_id"]) + ';'
    # match seller
    graql_insert_query += ' $seller isa person, has person_id ' + str(broker_relation["broker_seller_id"]) + ';'
    # insert broker_relation
    graql_insert_query += " insert $borker_relation(broker_buyer: $buyer, broker_seller: $seller) isa broker_relation; "
    return graql_insert_query

def website_relation_template(website_relation):
    # match company
    graql_insert_query = 'match $company isa company, has company_id ' + str(website_relation["company_id"]) + ';'
    # match website
    graql_insert_query += ' $website isa website, has url "' + website_relation["website_url"] + '";'
    # insert website_relation
    graql_insert_query += " insert $website_relation(company_website_owner: $company, company_website: $website) isa website_relation; "
    return graql_insert_query

def tag_relation_template(tag_relation):
    # match tag
    graql_insert_query = 'match $tag isa tag, has name "' + tag_relation["website_tag_name"] + '";'
    # match website
    graql_insert_query += ' $website isa website, has url "' + tag_relation["website_tag_owner_url"] + '";'
    # insert tag_relation
    graql_insert_query += " insert $tag_relation(website_tag: $tag, website_tag_owner: $website) isa tag_relation; "
    return graql_insert_query

def product_broker_relation_template(product_broker_relation):
    # match broker
    graql_insert_query = 'match $broker isa person, has person_id ' + str(product_broker_relation["broker_product_owner_id"]) + ';'
    # match product
    graql_insert_query += " $product isa product, has product_id " + str(product_broker_relation["broker_product_id"]) + ';'
    # insert product_broker_relation
    graql_insert_query += " insert $product_broker_relation(broker_product_owner: $broker, broker_product: $product) isa product_broker_relation; "
    return graql_insert_query

def product_company_relation_template(product_company_relation):
    # match company
    graql_insert_query = 'match $company isa company, has company_id ' + str(product_company_relation["company_product_owner_id"]) + ';'
    # match product
    graql_insert_query += " $product isa product, has product_id " + str(product_company_relation["company_product_id"]) + ';'
    # insert product_company_relation
    graql_insert_query += " insert $product_company_relation(company_product_owner: $company, company_product: $product) isa product_company_relation; "
    return graql_insert_query

def parse_data_to_dictionaries(input):
    items = []
    with open(input["data_path"] + ".json") as data:
        for item in ijson.items(data, "item"):
            items.append(item)
    return items

inputs = [
    {
        "data_path": "files/data/companies",
        "template": company_template
    },
    {
        "data_path": "files/data/people",
        "template": person_template
    },
    {
        "data_path": "files/data/websites",
        "template": website_template
    },
    {
        "data_path": "files/data/broker_relations",
        "template": broker_relation_template
    },
    {
        "data_path": "files/data/employList",
        "template": employ_template
    },
    {
        "data_path": "files/data/product_broker_relations",
        "template": product_broker_relation_template
    },
    {
        "data_path": "files/data/product_company_relations",
        "template": product_company_relation_template
    },
    {
        "data_path": "files/data/website_relations",
        "template": website_relation_template
    },
    {
        "data_path": "files/data/tag_relations",
        "template": tag_relation_template
    },
    {
        "data_path": "files/data/tags",
        "template": tag_template
    },
    {
        "data_path": "files/data/products",
        "template": product_template
    }
]

build_mineral_schema_graph(inputs)
