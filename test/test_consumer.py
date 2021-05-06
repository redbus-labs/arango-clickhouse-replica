from datetime import datetime

from replication.consumer.loader import pre_process_documents


def test_pre_process_documents():
    docs = [
        {"doc": {"tick": "100", "type": 2300, "db": "customerfeedback", "cuid": "c4010527", "tid": "5559734947",
                 "data": {"_key": "1", "_id": "AliasRoutes/145126835", "_rev": "_cQ3nO_W--_", "name": "t1",
                          "attr1": 1, "attr2": 1
                          }
                 },
         "offset": 1},
        {"doc": {"tick": "101", "type": 2300, "db": "customerfeedback", "cuid": "c4010527", "tid": "5559734947",
                 "data": {"_key": "2", "_id": "AliasRoutes/145126835", "_rev": "_cQ3nO_W--_", "name": "t1",
                          "attr1": 2, "attr2": 2
                          }
                 },
         "offset": 2},
        {"doc": {"tick": "102", "type": 2302, "db": "customerfeedback", "cuid": "c4010527", "tid": "5559734947",
                 "data": {"_key": "3", "_id": "AliasRoutes/145126835", "_rev": "_cQ3nO_W--_", "name": "t1",
                          "attr1": 3, "attr2": 3
                          }
                 },
         "offset": 3},
        {"doc": None, "offset": 4},
    ]
    initial_tick = "101"
    today = datetime.utcnow().strftime('%Y%j')
    result = pre_process_documents(initial_tick, docs)
    expected = [
        {"_key": "2", "_id": "AliasRoutes/145126835", "_rev": "_cQ3nO_W--_", "name": "t1", "attr1": 2, "attr2": 2,
         "_ver": int(f"{today}2"), "_deleted": 0},
        {"_key": "3", "_id": "AliasRoutes/145126835", "_rev": "_cQ3nO_W--_", "name": "t1", "attr1": 3, "attr2": 3,
         "_ver": int(f"{today}3"), "_deleted": 1},
    ]
    assert result == expected
