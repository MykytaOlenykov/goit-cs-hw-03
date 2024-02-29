from bson.objectid import ObjectId
from bson.errors import InvalidId
from flask import Flask, request, make_response
from connect_db import create_connect
from service import CatService
from classes import ModelValidateError


client = create_connect()
db = client["db-cats"]
cats_collection = db["cats"]

cat_service = CatService(cats_collection)

app = Flask("cats_api")


@app.route("/cats", methods=["GET"])
def get_cats():
    name = request.args.get("name")
    cats = cat_service.get_all(name)
    cats_json = [{**cat, "_id": str(cat["_id"])} for cat in cats]
    return cats_json, 200


@app.route("/cats/<id>", methods=["GET"])
def get_cat_by_id(id):
    try:
        cat = cat_service.get_by_id(ObjectId(id))
        if cat is None:
            return {"message": "Not found"}, 404
        return {**cat, "_id": str(cat["_id"])}, 200
    except InvalidId:
        return {"message": f"{id} invalid id"}, 400


@app.route("/cats", methods=["POST"])
def create_cat():
    try:
        cat_data = request.json
        cat = cat_service.create(cat_data)
        return {**cat, "_id": str(cat["_id"])}, 201
    except ModelValidateError as er:
        return {"message": er.message}, 400


@app.route("/cats/<id>", methods=["PUT"])
def update_cat_by_id(id):
    try:
        cat_data = request.json
        cat = cat_service.update_by_id(ObjectId(id), cat_data)
        if cat is None:
            return {"message": "Not found"}, 404
        return {**cat, "_id": str(cat["_id"])}, 200
    except InvalidId:
        return {"message": f"{id} invalid id"}, 400
    except ModelValidateError as er:
        return {"message": er.message}, 400


@app.route("/cats/<id>/features", methods=["PATCH"])
def add_cat_feature_by_id(id):
    try:
        cat_data = request.json
        feature = dict(cat_data).get("feature")
        cat = cat_service.add_feature_by_id(ObjectId(id), feature)
        if cat is None:
            return {"message": "Not found"}, 404
        return {**cat, "_id": str(cat["_id"])}, 200
    except InvalidId:
        return {"message": f"{id} invalid id"}, 400
    except ModelValidateError as er:
        return {"message": er.message}, 400


@app.route("/cats/<id>", methods=["DELETE"])
def delete_cat_by_id(id):
    try:
        cat_id = cat_service.delete_by_id(ObjectId(id))
        if cat_id is None:
            return {"message": "Not found"}, 404
        return {"_id": str(cat_id)}, 200
    except InvalidId:
        return {"message": f"{id} invalid id"}, 400


@app.route("/cats", methods=["DELETE"])
def delete_all_cats():
    cat_service.delete_all()
    response = make_response()
    response.status_code = 204
    return response


if __name__ == "__main__":
    app.run(host="localhost", port="8888", debug=True)
