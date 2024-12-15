
create_train = """
create or replace table train (label integer, text string)
as
select
    json_obj:label::integer AS label,
    json_obj:text::string AS text,
from
    train_json;"""

create_test = """
create or replace table test (label integer, text string, id integer)
as
select
    json_obj:label::integer AS label,
    json_obj:text::string AS text,
    (row_number() over (order by text)) as integer -- create id for each sentence
from
    test_json;"""

# COMPUTE DENOMINATOR FOR EACH LABEL ------------------------------------------------

compute_priori = """
create or replace table priori (label integer, priori float)
as
select label, (log(10, (count(*) / (select count(*) from train)) + 1.0e-37)) as prior
from train
group by label;"""

get_labels = """
create or replace table labels (label integer)
as
select * from (values (0), (1), (2), (3), (4));
"""

clean_tokenize_train = r"""
create or replace table word_label (label integer, word string)
as
select t.label, words.value as word
from train as t,
lateral strtok_split_to_table(lower(t.text), '\t\n\r !"#$%&\'()*+,-./0123456789:;<=>?@[\\]^_{|}~') as words;
"""

label_word_cross_product = """
create or replace table label_word_product (label integer, word string)
as
select l.label, word
from labels as l
cross join (select distinct wl.word from word_label as wl);"""

label_word_counts = """
create or replace table label_word_counts (label integer, word string, count integer)
as
select wlp.label, wlp.word, count(wl.word) as count
from label_word_product as wlp
left join word_label as wl on wl.word = wlp.word and wl.label = wlp.label
group by wlp.label, wlp.word;
"""

vocabulary_size = """
set vocabulary_size = (select count(distinct word) as size from word_label);
"""

compute_denominator_per_label = """
create or replace table label_denominator (label integer, denominator integer)
as
select lwc.label, (sum(lwc.count) + $vocabulary_size) as denominator
from label_word_counts lwc
group by lwc.label;
"""

# Making predictions  -------------------------------------------------------------------------------

clean_tokenize_test = r"""
create or replace table test_word_label_id (id integer, label integer, word string)
as
select t.id, t.label, words.value as word
from test as t,
lateral strtok_split_to_table(lower(t.text), '\t\n\r !"#$%&\'()*+,-./0123456789:;<=>?@[\\]^_{|}~') as words;
"""

compute_likelyhoods = """
create or replace table likelyhoods (id integer, predicted_label integer, true_label integer, likelyhood float)
as
select id, lwc.label as predicted_label, twli.label as true_label, sum(log(10, ((lwc.count + 1.0) /ld.denominator) + 1.0e-37)) as likelyhood
from test_word_label_id as twli
inner join label_word_counts as lwc on twli.word = lwc.word -- inner join to drop unknown words
left join label_denominator as ld on lwc.label = ld.label
group by id, lwc.label, twli.label;
"""

compute_scores = """
create or replace table scores (id integer, predicted_label integer, true_label integer, score float)
as
select l.id, l.predicted_label, l.true_label, (l.likelyhood + p.priori) as score
from likelyhoods as l
left join priori as p on l.predicted_label = p.label;
"""

compute_predictions = """
create or replace table predictions (predicted_label integer, id integer)
as
select MAX_BY(predicted_label, score) as predicted_label, scores.id
from scores
group by scores.id;
"""

# Computing accuracy  -------------------------------------------------------------------------------

compute_metrics = """
select TP, FP, TN, FN,
TP * 1.0 / (TP + FP) as precision,
TP * 1.0 / (TP + FN) as recall,
(TP + TN) * 1.0 / (TP + TN + FP + FN) as accuracy
from 
(select
sum(case when true_bin = 1 and pred_bin = 1 then 1 else 0 end) as TP,
sum(case when true_bin = 0 and pred_bin = 1 then 1 else 0 end) as FP,
sum(case when true_bin = 0 and pred_bin = 0 then 1 else 0 end) as TN,
sum(case when true_bin = 1 and pred_bin = 0 then 1 else 0 end) as FN
from
  (select
    case when actual_label > 1 then 1 else 0 end as true_bin,
    case when predicted_label > 1 then 1 else 0 end as pred_bin
    from
    (select t.label as actual_label, p.predicted_label, t.id
      from test t
      join predictions p on t.id = p.id
    ) as true_pred_labels
  ) as binary_labels
) as conf_matrix;
"""
