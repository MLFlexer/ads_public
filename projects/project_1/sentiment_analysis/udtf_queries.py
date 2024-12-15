def_get_priori = """
create or replace function get_priori(json_obj variant)
returns table (priori array)
language python
runtime_version=3.11
handler='GetPriori'
as $$
import math

class GetPriori:
    def __init__(self):
        self.label_count = [0]*5

    def process(self, json_dict):
        self.label_count[json_dict["label"]] += 1

    def end_partition(self):
        total_count = sum(self.label_count)
        result = []
        for label, count in enumerate(self.label_count):
            result.append(math.log((1 + count)/total_count))
        
        yield(result,)
$$;
"""

get_priori = """
create or replace table priori
as
select p.*
from train_raw as st,
table(get_priori(st.json_obj) over (partition by 1)) as p;
"""

def_get_word_label_count = """
create or replace function get_word_label_count(json_obj variant)
returns table (word text, label_counts array)
language python
runtime_version=3.11
handler='GetWordLabelCount'
as $$
import re

class GetWordLabelCount:
    def __init__(self):
        self.word_label_count_dict = {}

    def clean_text(self, text):
        words = []
        for word in re.split(r'[^\w]+', text):
            if word:
                words.append(word.lower())
        return words
        
    def process(self, json_dict):
        words = self.clean_text(json_dict["text"])

        for word in words:
            label_counts = self.word_label_count_dict.get(word)
            if label_counts:
                label_counts[json_dict["label"]] += 1
            else:
                label_counts = [0]*5
                label_counts[json_dict["label"]] += 1
                self.word_label_count_dict[word] = label_counts

    def end_partition(self):
        for word, label_counts in self.word_label_count_dict.items():
            yield(word, label_counts)
$$;
"""

get_word_label_count = """
create or replace table word_label_counts
as
select wlc.*
from train_raw as st,
table(get_word_label_count(st.json_obj) over (partition by 1)) as wlc;
"""

def_get_denominator = """
create or replace function get_denominator(label_counts array)
returns table (label_denominator array)
language python
runtime_version=3.11
handler='GetDenominator'
as $$
class GetDenominator:
    def __init__(self):
        self.counts = [0]*5
        self.vocab_size = 0
        
    def process(self, label_counts):
        self.vocab_size += 1
        for i in range(5):
            self.counts[i] += label_counts[i]

    def end_partition(self):
        for i in range(5):
            self.counts[i] += self.vocab_size
        yield(self.counts,)
$$;
"""

get_denominator = """
create or replace table denominator
as
select d.*
from word_label_counts as wlc,
table(get_denominator(wlc.label_counts) over (partition by 1)) as d;
"""

get_test_w_id = """
create or replace table test_w_id as
select row_number() over (order by null) as id, json_obj
from test_raw;
"""

def_get_tokenized_test = """
create or replace function get_tokenized_test(json_obj variant, id integer)
returns table (id integer, word text)
language python
runtime_version=3.11
handler='GetTokenizedTest'
as $$
import re

class GetTokenizedTest:        
    def process(self, json_dict, id):
        for word in re.split(r'[^\w]+', json_dict["text"]):
            yield(id, word)
$$;
"""

get_test_word_counts = """
create or replace table test_word_counts
as
select tok.*, wlc.label_counts
from
(select tok.*
from test_w_id as t,
table(get_tokenized_test(t.json_obj, t.id) over (partition by t.id)) as tok) as tok
right join word_label_counts as wlc on tok.word = wlc.word;
"""

def_get_likelyhoods = """
create or replace function get_likelyhoods(id integer, word_label_counts array, label_denominator array)
returns table (id integer, likelyhoods array)
language python
runtime_version=3.11
handler='GetLikelyHood'
as $$
import math

class GetLikelyHood:
    def __init__(self):
        self.likelyhoods = [0]*5
        self.id = 0
        
    def process(self, id, word_label_count, label_denominator):
        self.id = id
        for i in range(5):
            self.likelyhoods[i] += math.log((word_label_count[i] + 1) / label_denominator[i])

    def end_partition(self):        
        yield(self.id, self.likelyhoods)

$$;
"""

get_likelyhoods = """
create or replace table likelyhoods
as
select l.*
from test_word_counts as twc,
denominator as d,
table(get_likelyhoods(twc.id, twc.label_counts, d.label_denominator) over (partition by twc.id)) as l;
"""


def_get_prediction = """
create or replace function get_prediction(id integer, likelyhoods array, prioris array)
returns table (id integer, pred_label integer)
language python
runtime_version=3.11
packages = ('numpy')
handler='GetPrediction'
as $$
import numpy as np

class GetPrediction:        
    def process(self, id, likelyhoods, prioris):
        scores = [0]*5
        for i in range(5):
            scores[i] = likelyhoods[i] + prioris[i]
        yield(id, np.argmax(scores))
$$;
"""

get_prediction = """
create or replace table predictions
as
select pred.*
from likelyhoods as l,
priori as p,
table(get_prediction(l.id, l.likelyhoods, p.priori) over (partition by l.id)) as pred;
"""

def_get_metrics_bin = """
create or replace function get_metrics_bin(true_label integer, pred_label integer)
returns table (TP integer, FP integer, TN integer, FN integer, precision float, recall float, accuracy float)
language python
runtime_version=3.11
handler='GetMetrics'
as $$
class GetMetrics:
    def __init__(self):
        self.TP = 0
        self.FP = 0
        self.TN = 0
        self.FN = 0
        
    def process(self, true_label, pred_label):
        true_bin = 1 if true_label > 1 else 0
        pred_bin = 1 if pred_label > 1 else 0
        
        if true_bin == 1:
            if pred_bin == 1:
                self.TP += 1
            else:
                self.FN += 1
        else:
            if pred_bin == 1:
                self.FP += 1
            else:
                self.TN += 1
                
    def end_partition(self):
        precision = self.TP / (self.TP + self.FP)
        recall = self.TP / (self.TP + self.FN)
        accuracy = (self.TP + self.TN) / (self.TP + self.TN + self.FP + self.FN)
        yield(self.TP, self.FP, self.TN, self.FP, precision, recall, accuracy)  
$$;"""

get_metrics_bin = """
select m.*
from 
(select t.json_obj:label::integer as true_label, p.pred_label
from predictions as p
join test_w_id as t on t.id = p.id
) as p,
table(get_metrics_bin(p.true_label, p.pred_label) over (partition by 1)) as m;"""
