from deep_translator import GoogleTranslator


class DemoTranslator:

    def __init__(self):
        print("Google Translator initialized successfully.")

    def detect_topic(self, text):
        text_l = text.lower()

        if "cloud" in text_l or "server" in text_l:
            return "cloud"

        if "rabbitmq" in text_l or "queue" in text_l:
            return "streaming"

        if "bert" in text_l or "model" in text_l or "translation" in text_l:
            return "ai"

        if "student" in text_l or "students" in text_l:
            return "education"

        return "general"

    def translate(self, text, lang_code):

        translated_text = GoogleTranslator(
            source="en",
            target=lang_code
        ).translate(text)

        topic = self.detect_topic(text)

        return {
            "translated_text": translated_text,
            "semantic_topic": topic,
            "nlp_feature": "Google Translator"
        }