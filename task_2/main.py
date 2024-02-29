from connect_db import create_connect
from service import CatService

if __name__ == "__main__":
    client = create_connect()
    db = client["db-cats"]
    cats_collection = db["cats"]

    cat_service = CatService(cats_collection)
    # print(
    #     cat_service.create(
    #         {
    #             "name": "barsik",
    #             "age": 3,
    #             "features": ["ходить в капці", "дає себе гладити", "рудий"],
    #         }
    #     )
    # )
    # print(cat_service.get_all("a"))
    # print(type(cat_service.delete_by_id(ObjectId("65e061ca2f24fcb06500ed30"))))
    cat_service.delete_all()
