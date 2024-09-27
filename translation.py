from easynmt import EasyNMT

model = EasyNMT('opus-mt', cache_folder='./language_cache')

def get_translation(text):
    return model.translate(text, target_lang='en')