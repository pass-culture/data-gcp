from model import RiiModel
import sys, os

if __name__ == "__main__":
    rii_model = RiiModel()
    rii_model.set_up_model()
    rii_model.set_up_item_indexes()
    rii_model.set_up_index()
    rii_model.save("./metadata/rii.pkl")
    sys.exit(os.EX_OK)
