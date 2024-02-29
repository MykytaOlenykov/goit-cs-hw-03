from pymongo.collection import Collection
from bson.objectid import ObjectId
from model import CatModel
from connect_db import create_connect
from classes import ModelValidateError


class CatService:
    def __init__(self, db_collection: Collection) -> None:
        self.db_collection = db_collection

    def get_all(self, name: str | None = None) -> list[dict]:
        if name:
            return list(self.db_collection.find({"name": {"$regex": name}}))
        return list(self.db_collection.find())

    def get_by_id(self, id: ObjectId) -> dict | None:
        return self.db_collection.find_one({"_id": id})

    def create(self, body: dict) -> dict:
        new_cat = CatModel(body.get("name"), body.get("age"), body.get("features"))
        new_cat.validate()
        result_one = self.db_collection.insert_one(new_cat.to_dict())
        return self.get_by_id(result_one.inserted_id)

    def update_by_id(self, id: ObjectId, body: dict) -> dict | None:
        name = body.get("name")
        age = body.get("age")
        features = body.get("features")

        not name is None and CatModel.validate_name(name)
        not age is None and CatModel.validate_age(age)
        not features is None and CatModel.validate_features(features)

        data = dict()

        if not name is None:
            data["name"] = name
        if not age is None:
            data["age"] = age
        if not features is None:
            data["features"] = features

        updated_cat = self.db_collection.find_one_and_update(
            {"_id": id},
            {"$set": data},
            return_document=True,
        )

        return updated_cat

    def add_feature_by_id(self, id: ObjectId, new_feature: str) -> dict | None:
        if not new_feature:
            raise ModelValidateError("Field 'feature' is required.")
        if not isinstance(new_feature, str):
            raise ModelValidateError("Field 'feature' must be a string.")
        if len(new_feature) < 2:
            raise ModelValidateError(
                "Field 'feature' must be at least 2 characters long."
            )

        updated_cat = self.db_collection.find_one_and_update(
            {"_id": id},
            {"$push": {"features": new_feature}},
            return_document=True,
        )

        return updated_cat

    def delete_by_id(self, id: ObjectId) -> ObjectId | None:
        result = self.db_collection.delete_one({"_id": id})
        return id if result.deleted_count == 1 else None

    def delete_all(self) -> None:
        self.db_collection.delete_many({})


if __name__ == "__main__":
    client = create_connect()
    db = client["db-cats"]
    cats_collection: Collection = db["cats"]

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
    print(cat_service.get_all("a"))
    # print(type(cat_service.delete_by_id(ObjectId("65e061ca2f24fcb06500ed30"))))
