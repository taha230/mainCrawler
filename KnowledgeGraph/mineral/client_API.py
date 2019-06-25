from grakn.client import GraknClient

with GraknClient(uri="localhost:48555") as client:
    with client.session(keyspace="mineral") as session:
        ## Insert a Person using a WRITE transaction
        #with session.transaction().write() as write_transaction:
        #    insert_iterator = write_transaction.query('insert $x isa person, has email "x@email.com";')
        #    concepts = insert_iterator.collect_concepts()
        #    print("Inserted a person with ID: {0}".format(concepts[0].id))
        #    ## to persist changes, write transaction must always be committed (closed)
        #    write_transaction.commit()

        ## Read the person using a READ only transaction
        with session.transaction().read() as read_transaction:
            answer_iterator = read_transaction.query("match $x isa person, has email 'ce_bo@yahoo.com'; get; limit 10;")

            for answer in answer_iterator:
                person = answer.map()["x"]
                print("Retrieved person with id " + person.id)
                attrs = person.attributes()
                for each in attrs:
                    print(each.type().label())
                    print(each.value())
                    print("++++++++++++++++++++++++++++++++++++++++++++")

        ## Or query and consume the iterator immediately collecting all the results
      #  with session.transaction().read() as read_transaction:
      #      answer_iterator = read_transaction.query("match $x isa person; get; limit 10;")
      #      persons = answer_iterator.collect_concepts()
      #      for person in persons:
      #          print("Retrieved person with first_name "+ person["first_name"])

        ## if not using a `with` statement, then we must always close the session and the read transaction
        # read_transaction.close()
        # session.close()
        # client.close()


