import os
from huggingface_hub import HfApi

from config import RESULT_DIR

api = HfApi(token=os.getenv("HF_TOKEN"))

api.upload_folder(
    folder_path=RESULT_DIR,
    repo_id="jayjayjet/merchant-category-codes",
    repo_type="dataset",
)
