from pydantic import BaseModel

BaseModel.model_config["coerce_numbers_to_str"] = True
