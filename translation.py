from easynmt import EasyNMT

model = EasyNMT('opus-mt', cache_folder='./language_cache')
#model = EasyNMT('./m2m_100_1.2B')

enabled_langs = ["af", "ar", "az", "bg", "bi", "bn", "ca", "cs", "cy", "da", "de", "ee", "eo", "es", "et", "eu", "fi", "fj", "fr", "ga", "gl", "gv", "ha", "hi", "ho", "ht", "hu", "hy", "id", "ig", "is", "it", "ja", "ka", "kg", "kj", "kl", "ko", "lg", "ln", "lu", "lv", "mg", "mh", "mk", "ml", "mr", "mt", "ng", "nl", "ny", "om", "pa", "pl", "rn", "ru", "rw", "sg", "sk", "sm", "sn", "sq", "ss", "st", "sv", "th", "ti", "tl", "tn", "to", "tr", "ts", "uk", "ur", "ve", "vi", "wa", "xh", "yo", "zh"]

def get_translation(detected_lang, text):
    if detected_lang in enabled_langs:
        return model.translate(text, target_lang='en', source_lang=detected_lang)
    else:
        return None