class ModelRegistry:

    def __init__(self) -> None:
        self.models = {}

    def register_model(self, model_name, model):
        self.models[model_name] = model

    def get_model(self, model_name):
        return self.models[model_name]

    def clear(self):
        self.models.clear()


model_registry = ModelRegistry()
