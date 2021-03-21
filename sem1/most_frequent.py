from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
 
    def combiner_count_words(self, word, counts):
        # sum the words we've seen so far
        yield (word, sum(counts))
 
    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)
 
    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        with open('/Users/alan-gasiev/Documents/bigdata_msu/debug.log', 'w') as file:
            print(word_count_pairs, file=file)
        yield ('frequent', sorted(word_count_pairs, reverse=True)[:5])
 
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
            combiner=self.combiner_count_words,
            reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()