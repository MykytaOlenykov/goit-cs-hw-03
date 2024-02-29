from classes import ModelValidateError


class CatModel:
    def __init__(self, name, age, features=None):
        if not features:
            features = []

        self.name = name
        self.age = age
        self.features = features

    @staticmethod
    def from_dict(data):
        return CatModel(data["name"], data["age"], data["features"])

    @staticmethod
    def validate_name(name):
        if not name:
            raise ModelValidateError("Field 'name' is required.")
        if not isinstance(name, str):
            raise ModelValidateError("Field 'name' must be a string.")
        if len(name) < 2:
            raise ModelValidateError("Field 'name' must be at least 2 characters long.")

    @staticmethod
    def validate_age(age):
        if not age:
            raise ModelValidateError("Field 'age' is required.")
        if not isinstance(age, int):
            raise ModelValidateError("Field 'age' must be an integer.")
        if age < 0:
            raise ModelValidateError("Field 'age' must be greater than to 0.")

    @staticmethod
    def validate_features(features):
        if not isinstance(features, list):
            raise ModelValidateError("Field 'features' must be a list.")
        for feature in features:
            if not isinstance(feature, str):
                raise ModelValidateError("Field 'features' must contain only strings.")
            if len(feature) < 2:
                raise ModelValidateError(
                    "Field 'features' must contain strings with a minimum length of 2 characters."
                )

    def to_dict(self):
        return {"name": self.name, "age": self.age, "features": self.features}

    def validate(self):
        CatModel.validate_name(self.name)
        CatModel.validate_age(self.age)
        CatModel.validate_features(self.features)


if __name__ == "__main__":
    new_cat = CatModel("barsik", 3, ["ходить в капці", "дає себе гладити", "рудий"])
    new_cat.validate()
    print(new_cat.to_dict())
    print(CatModel.from_dict(new_cat.to_dict()))
