import spacy
from spacy.lang.en.stop_words import STOP_WORDS
import re

# text = '''
# Requirements:

# - Advanced degree in Data Science, Bioinformatics, Computational Biology, Genomics, or a related field (or equivalent work experience). Ph.D. + 1 year of experience or Masters degree with 6 years of experience
# - Solid understanding of molecular biology, genomics, and next-generation sequencing (NGS) technologies with demonstrated ability to evaluate and select bioinformatics tools and build data analysis pipelines for NGS assays.
# - Familiarity with CRISPR based gene editing technologies and assays is a plus.
# - Demonstrated experience with bioinformatics tools and software (e.g., Samtools, BAM tools, GATK and other state-of-the-art variant calling tools for small indels, SNPs and large structural variations, etc.).
# - Ability to independently analyze complex / multidimensional genomic data, create visualizations and drive decision making, storytelling through presentation of data.
# - Strong proficiency in Python programming language. Additional languages including R are a plus. Proficiency with the Linux OS and command-line environment.
# - Familiarity with version control and code management sch as Git.
# - Familiarity with high-performance cloud platforms and related tools for high throughput computing. (e.g., AWS, NextFlow, Batch)
# - Strong problem-solving skills with outside the box thinking and attention to detail.
# - Excellent communication skills and the ability to work in a collaborative, interdisciplinary environment on an on-going basis.
# - Communicate results effectively through reports and presentations to internal stakeholders.
# - Assist in generating material to present findings to external clients and contribute to preparation of scientific publications.
# - Ability to manage multiple projects and deadlines in a fast-paced setting. 
# the use of Tensorflow Extended is necessary. Apart from that node.js and node and ariflow will also be used.
# We offer a competitive base salary, annual bonus, and equity. MaxCyte also offers a comprehensive benefits package including health, dental, vision, life, and disability insurance and generous time off.
# MaxCyte is an Equal Opportunity Employer. All qualified applicants will receive consideration for employment without regard to race, color, sex, sexual orientation, gender identity, religion, national origin, disability, veteran status, age, marital status, pregnancy, genetic information, or other legally protected status.

# '''
class noun:
    def __init__(self,text):
        self.text = text
        self.result = self.extract_nouns()
   

    def extract_nouns(self):
        text = self.text
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)

        noun_phrases = []
        for chunk in doc.noun_chunks:
            cleaned_phrase = " ".join([word.text for word in chunk if word.text.lower() not in STOP_WORDS])
            if cleaned_phrase:
                noun_phrases.append(cleaned_phrase)

        # print("Filtered Noun Phrases (stopwords removed):", set(noun_phrases))

        total_words = sum(len(phrase.split()) for phrase in noun_phrases)
        # print("Total number of words in noun phrase list:", total_words)

        generic_pattern = re.compile(r"\b[\w\-]+\.[\w\-]+\b|\b[\w\-]+\-[\w\-]+\b")
        regex_matches = generic_pattern.findall(text)
        # print("Regex matches from text:", regex_matches)

        final = noun_phrases + regex_matches

        return final



# n = noun(text)
# print(n.result)

