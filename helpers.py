import os
import json

def load_json_files(src):
    combined_data = {}
    for filename in os.listdir(src):
        if filename.endswith('.json'):
            file_path = os.path.join(src, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                combined_data.update(data)
    return combined_data