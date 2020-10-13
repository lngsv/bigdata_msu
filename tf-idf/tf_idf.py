import datetime
import math
import os
import re
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol


WORD_RE = re.compile(r"[\w']+")


def _log(text):
    cwd = os.path.abspath(os.getcwd())
    with open(os.path.join(cwd, 'debug.log'), 'a') as file:
        print(datetime.datetime.now().isoformat(), '\t', str(text), file=file)


class MRTFIDF(MRJob):

    OUTPUT_PROTOCOL = JSONValueProtocol

    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg('--search_request', '-s', default='')
        self.add_passthru_arg(
            '--response_size',
            '-n',
            type=int,
            default=5,
            help='Maximum number of results',
        )
        self.add_passthru_arg(
            '--docbase_size',
            '-d',
            type=int,
            required=True,
            help='Number of docs being passed',
        )

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_words,
                combiner=self.reducer_sum_counts,
                reducer=self.reducer_sum_counts,
            ),
            MRStep(
                mapper=self.mapper_regroup_word_doc,
                reducer=self.reducer_word_count,
            ),
            MRStep(
                mapper=self.mapper_regroup_word_freqs,
                reducer=self.reducer_sum_doc_counts,
            ),
            MRStep(
                mapper=self.mapper_compute_word_tf_idf,
                reducer=self.reducer_compute_doc_tf_idf,
            ),
            MRStep(reducer=self.reducer_compute_result),
        ]

    # line => [(word, doc), 1]
    def mapper_get_words(self, _, line):
        filename = os.environ['map_input_file']
        for word in WORD_RE.findall(line):
            yield (word, filename), 1

    # (word, doc), [ki] => (word, doc), n
    def reducer_sum_counts(self, word, counts):
        yield word, sum(counts)

    # (word, doc), n => word, (doc, n)
    def mapper_regroup_word_doc(self, word_doc, count):
        yield word_doc[1], (word_doc[0], count)

    # doc, [(word, n)] => [(word, doc), (n, N)]
    def reducer_word_count(self, doc, word_counts):
        word_counts = list(word_counts)
        N = sum(e[1] for e in word_counts)
        for word_count in word_counts:
            yield (word_count[0], doc), (word_count[1], N)

    # (word, doc), (n, N) => word, (doc, n, N, 1)
    def mapper_regroup_word_freqs(self, word_doc, freq):
        yield word_doc[0], (word_doc[1], freq[0], freq[1], 1)

    # word, [(doc, n, N, 1)] => [word, (doc, n, N, m)]
    def reducer_sum_doc_counts(self, word, doc_counts):
        doc_counts = list(doc_counts)
        m = len(doc_counts)
        for doc, n, N, _ in doc_counts:
            yield word, (doc, n, N, m)

    # word, (doc, n, N, m) + D => doc, (word, TF*IDF)
    def mapper_compute_word_tf_idf(self, word, count):
        doc, n, N, m = count
        tf = n / N
        idf = math.log(self.options.docbase_size / m)
        yield doc, (word, tf * idf)

    # doc, [word, TF*IDF] + search_request => None, (doc, doc rate)
    def reducer_compute_doc_tf_idf(self, doc, words):
        request = set(WORD_RE.findall(self.options.search_request))
        if not request:
            yield None, (doc, 0)
            return
        sum_values = 0
        for word in words:
            if word[0] in request:
                sum_values += word[1]
        yield None, (doc, sum_values / len(request))

    # [(doc, doc rate)] => None, [sorted docs]
    def reducer_compute_result(self, _, values):
        yield None, [
            name[len('file://') :]
            for name, _ in sorted(
                list(values), key=lambda el: (-el[1], el[0]),
            )[: self.options.response_size]
        ]


if __name__ == '__main__':
    MRTFIDF.run()
