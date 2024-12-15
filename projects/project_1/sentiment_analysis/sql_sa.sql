create database camel_db_sql_3;
use database camel_db_sql_3;
-- add test json
 create table test_json (json_obj variant);
 copy into test_json from @~/staged/test-00000-of-00001.parquet 
 file_format=training_db.TPCH_SF1.MYPARQUETFORMAT;
 
-- add train json
 create table train_json (json_obj variant);
 copy into train_json from @~/staged/train-00000-of-00001.parquet
 file_format=training_db.TPCH_SF1.MYPARQUETFORMAT;

create or replace table train (label integer, text string)
as
SELECT
    json_obj:label::integer AS label,
    json_obj:text::string AS text,
FROM
    train_json;
    
create or replace table test (label integer, text string, id integer)
as
SELECT
    json_obj:label::integer AS label,
    json_obj:text::string AS text,
    (row_number() over (order by text)) as integer -- create id for each sentence
FROM
    test_json;
    
-- *******************
----- train operations:
-- *******************
-- Prior from training
-- input: id, label
-- output: label, priori
create or replace table priori (label integer, priori float)
as
select label, (log(10, count(*) / (select count(*) from train))) as prior
from train
group by label;

-- possible labels
create or replace table labels (label integer)
as
select * from (values (0), (1), (2), (3), (4));

-- clean and tokenize text
create or replace table word_label (label integer, word string)
as
select t.label, words.value as word
from train as t,
lateral strtok_split_to_table(lower(t.text), '\t\n\r !"#$%&\'()*+,-./0123456789:;<=>?@[\\]^_{|}~') as words;
-- '

-- Get cross product of labels and words, as not every word is present in every class
create or replace table label_word_product (label integer, word string)
as
select l.label, word
from labels as l
cross join (select distinct wl.word from word_label as wl);

-- Count number of words for a specific word, label combination
create or replace table label_word_counts (label integer, word string, count integer)
as
select wlp.label, wlp.word, count(wl.word) as count
from label_word_product as wlp
left join word_label as wl on wl.word = wlp.word and wl.label = wlp.label
group by wlp.label, wlp.word;

-- compute vocabulary size
set vocabulary_size = (select count(distinct word) as size from word_label);

-- compute denominator for each class when computing likelyhoods
create or replace table label_denominator (label integer, denominator integer)
as
select lwc.label, (sum(lwc.count) + $vocabulary_size) as denominator
from label_word_counts lwc
group by lwc.label;

-- *******************
----- Test operations:
-- *******************

-- clean, tokenize and give id. Note that unknown words are dropped in the likelyhoods query
create or replace table test_word_label_id (id integer, label integer, word string)
as
select t.id, t.label, words.value as word
from test as t,
lateral strtok_split_to_table(lower(t.text), '\t\n\r !"#$%&\'()*+,-./0123456789:;<=>?@[\\]^_{|}~') as words;
-- '

-- compute likelyhood for each word
create or replace table likelyhoods (id integer, predicted_label integer, true_label integer, likelyhood float)
as
select id, lwc.label as predicted_label, twli.label as true_label, sum(log(10, ((lwc.count + 1.0) /ld.denominator) + 1.0e-37)) as likelyhood
from test_word_label_id as twli
inner join label_word_counts as lwc on twli.word = lwc.word -- inner join to drop unknown words
left join label_denominator as ld on lwc.label = ld.label
group by id, lwc.label, twli.label;

create or replace table scores (id integer, predicted_label integer, true_label integer, score float)
as
select l.id, l.predicted_label, l.true_label, (l.likelyhood + p.priori) as score
from likelyhoods as l
left join priori as p on l.predicted_label = p.label;

create or replace table predictions (predicted_label integer, id integer)
as
select MAX_BY(predicted_label, score) as predicted_label, scores.id
from scores
group by scores.id;

-- *************************
-- Make predictions
-- *************************

-- exact prediction
select count(*) as total_correct, (select count(*) from test) as total, (count(*)/(select count(*) from test)) as accuracy
from predictions as p
left join test as t on p.id = t.id
where p.predicted_label = t.label;

-- binary prediction all labels
select count(*) as total_correct, (select count(*) from test) as total, (count(*)/(select count(*) from test)) as accuracy
from predictions as p
left join test as t on p.id = t.id
where (p.predicted_label <= 3 and t.label <= 3) or (p.predicted_label > 3 and t.label > 3);

-- binary prediction 0 and 4 labels
select count(*) as total_correct, (select count(*) from test where label = 0 or label = 4) as total, (count(*)/(select count(*) from test where label = 0 or label = 4)) as accuracy
from predictions as p
left join test as t on p.id = t.id
where p.predicted_label = t.label and (t.label = 0 or t.label = 4);

