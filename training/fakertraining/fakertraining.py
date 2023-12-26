import random

from faker import Faker
from faker.providers import BaseProvider, DynamicProvider

f = Faker()


class NeuralProvider(BaseProvider):
    def video_category(self):
        return random.choice(["Machine Learning", "Vim", "Linux", "Finance"])
    
    def video_title(self):
        return "TITLE"
    

f.add_provider(NeuralProvider)

print(f.video_category())
print(f.video_title())

programming_language_provider = DynamicProvider(
    provider_name="programming_language",
    elements=["Python", "Go", "JS", "Ruby", "C#"]
)

f.add_provider(programming_language_provider)
print(f.programming_language())
