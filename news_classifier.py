# news_classifier.py
class NewsClassifier:
    def __init__(self, model, vectorizer, label_encoder, threshold=0.5):
        self.model = model
        self.vectorizer = vectorizer
        self.label_encoder = label_encoder
        self.threshold = threshold

    def predict(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        max_prob = max(probs)
        pred_index = probs.argmax()
        if max_prob < self.threshold:
            return "기타"
        return self.label_encoder.inverse_transform([pred_index])[0]

    def predict_proba(self, text):
        X = self.vectorizer.transform([text])
        probs = self.model.predict_proba(X)[0]
        return dict(zip(self.label_encoder.classes_, map(float, probs)))
