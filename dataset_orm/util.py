import dataset

from dataset_orm import Model


def connect(url, recreate=False):
    # auto connect all registered models
    db = dataset.connect(url)
    setup_models(db, recreate=recreate)
    return db


def setup_models(db, recreate=False):
    for m in Model._all_models:
        m.use(db, recreate=recreate)
