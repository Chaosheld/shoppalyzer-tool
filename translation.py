from easynmt import EasyNMT

model = EasyNMT('opus-mt')

def get_translation(text):
    return model.translate(text, target_lang='en')